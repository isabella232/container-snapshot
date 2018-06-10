package snapshot

import (
	"sync"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/csi"
	"github.com/openshift/container-snapshot/pkg/notifier"
	dockernotifier "github.com/openshift/container-snapshot/pkg/notifier/docker"
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

	client := s.Client
	if client == nil {
		c, err := docker.NewClientFromEnv()
		if err != nil {
			return err
		}
		client = c
	}
	n := dockernotifier.New(client, s, snapshotFilter{})
	s.snapshotter = n
	if len(s.SnapshotFrom) > 0 {
		s.snapshotter = NewSnapshotFromFile(s.SnapshotFrom)
	}

	stopCh := make(chan struct{})
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
	select {}
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
