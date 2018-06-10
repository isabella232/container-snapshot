package snapshot

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/csi"
	"github.com/openshift/container-snapshot/pkg/notifier"
	dockernotifier "github.com/openshift/container-snapshot/pkg/notifier/docker"
	"github.com/openshift/container-snapshot/pkg/watcher"
)

type Server struct {
	Client *docker.Client

	MountDirectory string
	SnapshotFrom   string

	ListenCSIAddr     string
	CSIStateDirectory string

	snapshotter notifier.ContainerSnapshotter

	lock    sync.Mutex
	servers map[serverName]*containerDirectoryWatcher
}

type serverName struct {
	UID           string
	ContainerName string
}

func (s *Server) Start() error {
	if s.servers == nil {
		s.servers = make(map[serverName]*containerDirectoryWatcher)
	}

	stopCh := make(chan struct{})
	if err := s.init(stopCh); err != nil {
		return err
	}
	select {}
}

func (s *Server) init(stopCh chan struct{}) error {
	client := s.Client
	if client == nil {
		c, err := docker.NewClientFromEnv()
		if err != nil {
			return err
		}
		client = c
	}
	n := dockernotifier.New(client, s)
	s.snapshotter = n
	if len(s.SnapshotFrom) > 0 {
		s.snapshotter = NewSnapshotFromFile(s.SnapshotFrom)
	}

	if len(s.ListenCSIAddr) > 0 {
		go func() {
			csiServer := csi.New(s, s.CSIStateDirectory)
			glog.Fatalf("CSI server exited: %v", csiServer.Serve(s.ListenCSIAddr))
		}()
	} else {
		glog.Infof("Watching docker daemon for containers")
		if err := n.Run(stopCh); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) CSIMountAdded(podUID, path string) error {
	info := &notifier.ContainerInfo{
		PodUID:    podUID,
		MountPath: path,
	}
	s.MountAdded(info)
	return nil
}

func (s *Server) CSIMountRemoved(podUID, path string) error {
	info := &notifier.ContainerInfo{
		PodUID:    podUID,
		MountPath: path,
	}
	s.MountRemoved(info)
	return nil
}

func (s *Server) MountSync(infos []*notifier.ContainerInfo) {
	s.lock.Lock()
	defer s.lock.Unlock()
	missing := make(map[serverName]struct{})
	for k := range s.servers {
		if len(k.ContainerName) == 0 {
			continue
		}
		missing[k] = struct{}{}
	}
	for _, info := range infos {
		delete(missing, serverName{UID: info.PodUID, ContainerName: info.ContainerName})
	}
	for k := range missing {
		s.closeServer(k)
	}
	// TODO: other cleanup
}

func (s *Server) MountAdded(info *notifier.ContainerInfo) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.MountDirectory) > 0 {
		info.MountPath = s.MountDirectory
	}
	server := s.newServer(info)
	s.servers[serverName{UID: info.PodUID, ContainerName: info.ContainerName}] = server
	glog.Infof("Starting server on container %s from pod %s in namespace %s", info.ContainerName, info.PodName, info.PodNamespace)
	go func() {
		if err := server.Serve(); err != nil {
			glog.Errorf("Server startup failed on container %s from pod %s in namespace %s: %v", info.ContainerName, info.PodName, info.PodNamespace, err)
		}
	}()
}

func (s *Server) MountRemoved(info *notifier.ContainerInfo) {
	s.lock.Lock()
	defer s.lock.Unlock()
	name := serverName{UID: info.PodUID, ContainerName: info.ContainerName}
	s.closeServer(name)
}

func (s *Server) closeServer(name serverName) {
	server, ok := s.servers[name]
	if !ok {
		return
	}
	glog.Infof("Stopping server for container %s in pod %s", name.ContainerName, name.UID)
	if err := server.Close(); err != nil {
		glog.Errorf("Server shutdown reported error for container %s in pod %s: %v", name.ContainerName, name.UID, err)
	}
	delete(s.servers, name)
}

func (s *Server) newServer(info *notifier.ContainerInfo) *containerDirectoryWatcher {
	return &containerDirectoryWatcher{
		info:        info,
		snapshotter: s.snapshotter,
	}
}

type containerDirectoryWatcher struct {
	info        *notifier.ContainerInfo
	tracker     *containerSnapshotTracker
	snapshotter notifier.ContainerSnapshotter

	lock    sync.Mutex
	stopCh  chan struct{}
	stopped bool
}

func (s *containerDirectoryWatcher) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.stopped {
		return nil
	}
	s.stopped = true
	close(s.stopCh)
	s.stopCh = nil
	return nil
}

func (s *containerDirectoryWatcher) Serve() error {
	s.lock.Lock()
	if s.stopCh != nil {
		s.lock.Unlock()
		return fmt.Errorf("server is already started")
	}
	s.stopCh = make(chan struct{})
	s.stopped = false
	s.tracker = newContainerSnapshotTracker(s.snapshotter)
	go s.tracker.Run(s.stopCh)
	s.lock.Unlock()

	w := watcher.New([]string{s.info.MountPath}, func(paths []string) error {
		if err := s.refresh(s.info.MountPath); err != nil {
			glog.Errorf("Unable to refresh contents of directory %s for container %s in pod %s/%s: %v", err, s.info.MountPath, s.info.ContainerName, s.info.PodNamespace, s.info.PodName)
		}
		return nil
	})
	w.SetMinimumInterval(10 * time.Millisecond)
	w.SetMaxDelays(100)
	return w.Run(s.stopCh)
}

var (
	validContainerNameRegexp = regexp.MustCompile(`^[a-z0-9\-]+$`)
	resultFileSuffix         = ".json"
)

// refresh observes the contents of a directory to determine what snapshots are requested
// or have already completed.
//
// Users may specify either a named pipe or a regular file. If they specify a named pipe
// the contents of the container will be streamed to the pipe and then the pipe will be
// deleted. If they create a zero-byte file, the file contents will be replaced with
// the contents of the snapshot. After completion, a .json file will be written back
// to the directory with more details about the snapshot. The presence of a .json file
// alongside a zero-byte file will prevent a snapshot (the user must remove the .json
// file before creating the new file).
func (s *containerDirectoryWatcher) refresh(path string) error {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	if len(files) > 100 {
		return fmt.Errorf("container %s in pod %s (namespace: %s) has too many files in %s, no action taken", s.info.ContainerName, s.info.PodName, s.info.PodNamespace, s.info.MountPath)
	}

	states := make([]State, 0, len(files))
File:
	for _, file := range files {
		if file.IsDir() || file.Mode()&os.ModeSymlink == os.ModeSymlink {
			continue
		}

		parts := strings.SplitN(file.Name(), ".", 3)
		if len(parts) > 2 {
			continue
		}
		name := parts[0]
		parts = parts[1:]
		if !validContainerNameRegexp.MatchString(name) {
			continue
		}

		switch {
		// we process files in order, so the extension is always last
		case len(parts) == 1 && parts[0] == "json":
			if len(states) > 0 {
				previous := len(states) - 1
				if states[previous].ContainerName == name && !states[previous].Pipe {
					glog.V(4).Infof("Container %s was not a pipe and has a status file, marking as complete", name)
					states[previous].Completed = true
				}
			}

		case len(states) > 0 && states[len(states)-1].ContainerName == name:
			// if we see multiple versions of the container name, do nothing
			glog.V(4).Infof("File %s ignored", file.Name())

		default:
			if file.Size() > 0 {
				glog.V(4).Infof("Container %s was in a completed state", name)
				states = append(states, State{
					ContainerName: name,
					Completed:     true,
				})
				break
			}
			condition := notifier.ConditionSuccess
			if len(parts) > 0 {
				switch s := notifier.ConditionType(parts[0]); s {
				case notifier.ConditionFailed, notifier.ConditionDone, notifier.ConditionNode:
					condition = s
				default:
					glog.V(4).Infof("Container %s had unrecognized condition %q", name, s)
					continue File
				}
			}

			states = append(states, State{
				ContainerName: name,
				Filename:      file.Name(),
				Condition:     condition,
				Pipe:          file.Mode()&os.ModeNamedPipe == os.ModeNamedPipe,
			})
		}
	}

	return s.tracker.Sync(states, s.info.PodUID, path)
}
