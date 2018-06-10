package csi

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"github.com/pborman/uuid"

	"github.com/kubernetes-csi/drivers/pkg/csi-common"
)

type Snapshot struct {
	driver *csicommon.CSIDriver

	notifier *LocalStateNotifier

	identity   *identityServer
	node       *nodeServer
	controller *controllerServer

	accessModes  []*csi.VolumeCapability_AccessMode
	capabilities []*csi.ControllerServiceCapability
}

type MountNotifier interface {
	CSIMountAdded(podUID, path string) error
	CSIMountRemoved(podUID, path string) error
}

type LocalStateNotifier struct {
	path string

	lock     sync.Mutex
	notifier MountNotifier
}

func newLocalStateNotifier(path string, notifier MountNotifier) *LocalStateNotifier {
	return &LocalStateNotifier{
		notifier: notifier,
		path:     path,
	}
}

func (n *LocalStateNotifier) Init() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	if err := os.MkdirAll(n.path, 0700); err != nil {
		return err
	}
	files, err := ioutil.ReadDir(n.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() || file.Mode()&os.ModeSymlink != os.ModeSymlink {
			continue
		}
		podUID := file.Name()

		path := filepath.Join(n.path, file.Name())
		targetPath, err := os.Readlink(path)
		if err != nil {
			continue
		}

		if _, err := os.Stat(targetPath); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("unable to read target local state for uid %s: %v", podUID, err)
			}
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				glog.Errorf("Unable to remove missing pod symlink: %v", err)
			}
			continue
		}
		glog.Infof("Recovering pod mount %s to %s", podUID, targetPath)
		if err := n.notifier.CSIMountAdded(podUID, targetPath); err != nil {
			return err
		}
	}
	return nil
}

func (n *LocalStateNotifier) CSIMountAdded(podUID, path string) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	linkPath := filepath.Join(n.path, podUID)
	if err := os.Remove(linkPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unable to remove local state link for pod %s: %v", podUID, err)
	}
	if err := os.Symlink(path, linkPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("unable to store local state for sync plugin for pod %s: %v", podUID, err)
	}
	return n.notifier.CSIMountAdded(podUID, path)
}

func (n *LocalStateNotifier) CSIMountRemoved(podUID, path string) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	if err := os.Remove(filepath.Join(n.path, podUID)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("unable to clear local state for sync plugin for pod %s: %v", podUID, err)
	}
	return n.notifier.CSIMountAdded(podUID, path)

}

// New instantiates a new snapshot server.
func New(notifier MountNotifier, statePath string) *Snapshot {
	// Initialize default library driver
	s := &Snapshot{
		notifier: newLocalStateNotifier(statePath, notifier),
	}
	s.driver = csicommon.NewCSIDriver("container-snapshot.openshift.io", "0.0.1", uuid.New())
	if s.driver == nil {
		panic("unable to initialize driver")
	}
	s.driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	})
	s.driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	})

	// Create GRPC servers
	s.identity = &identityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(s.driver),
	}
	s.node = &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(s.driver),
		notifier:          s.notifier,
	}
	s.controller = &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(s.driver),
	}
	return s
}

func (s *Snapshot) Serve(endpoint string) error {
	if err := s.notifier.Init(); err != nil {
		return err
	}
	server := csicommon.NewNonBlockingGRPCServer()
	server.Start(endpoint, s.identity, s.controller, s.node)
	server.Wait()
	return nil
}

type volumes []Volume

func (v volumes) ByID(id string) (Volume, bool) {
	for _, snapshotVol := range v {
		if snapshotVol.ID == id {
			return snapshotVol, true
		}
	}
	return Volume{}, false
}

func (v volumes) ByName(name string) (Volume, bool) {
	for _, snapshotVol := range v {
		if snapshotVol.Name == name {
			return snapshotVol, true
		}
	}
	return Volume{}, false
}

type Volume struct {
	Name string `json:"volName"`
	ID   string `json:"ID"`
	Size int64  `json:"volSize"`
	Path string `json:"volPath"`
}
