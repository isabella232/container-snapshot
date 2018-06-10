package snapshot

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/notifier"
	"github.com/openshift/container-snapshot/pkg/watcher"
)

type snapshotFromFile struct {
	path string
}

func NewSnapshotFromFile(path string) notifier.ContainerSnapshotter {
	return &snapshotFromFile{path: path}
}

func (s *snapshotFromFile) Snapshot(condition notifier.ConditionType, podUID, containerName string) (io.ReadCloser, error) {
	return os.Open(s.path)
}

func (s *snapshotFromFile) Wait(condition notifier.ConditionType, podUID, containerName string) <-chan struct{} {
	return nil
}

type SnapshotRequestController interface {
	Sync(state []State, podUID, path string) error
}

// containerDirectoryWatcher observes a directory (providing the API for the daemon when not serving CSI)
// and notifies the tracker of the requested operations.
type containerDirectoryWatcher struct {
	info        *notifier.ContainerInfo
	tracker     SnapshotRequestController
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
	tracker := newContainerSnapshotTracker(s.snapshotter)
	// run two tracker workers
	for i := 0; i < 2; i++ {
		go tracker.Run(s.stopCh)
	}

	s.tracker = tracker
	s.stopped = false
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
				case notifier.ConditionFailed, notifier.ConditionDone, notifier.ConditionNow:
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
