package snapshot

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/glog"
)

type State struct {
	ContainerName string
	Completed     bool
	Pipe          bool
}

type containerSnapshotTracker struct {
	snapshotter ContainerSnapshotter
	work        chan struct{}
	lock        sync.Mutex
	operations  map[string]*containerSnapshotOp
}

func newContainerSnapshotTracker(snapshotter ContainerSnapshotter) *containerSnapshotTracker {
	return &containerSnapshotTracker{
		snapshotter: snapshotter,
		work:        make(chan struct{}),
		operations:  make(map[string]*containerSnapshotOp),
	}
}

func (c *containerSnapshotTracker) Run(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		case <-c.work:
			if err := c.runOnce(); err != nil {
				glog.Errorf(err.Error())
			}
		}
	}
}

func (c *containerSnapshotTracker) next() *containerSnapshotOp {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, op := range c.operations {
		if op.completed {
			continue
		}
		return op
	}
	return nil
}

func (c *containerSnapshotTracker) runOnce() error {
	op := c.next()
	if op == nil {
		return nil
	}
	defer func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		op.completed = true
	}()
	return op.Run()
}

func (c *containerSnapshotTracker) Sync(states []State, podUID, baseDir string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, op := range c.operations {
		op.clear = true
	}

	for _, state := range states {
		if op, ok := c.operations[state.ContainerName]; ok {
			op.clear = false
			op.completed = state.Completed
			continue
		}
		op := newContainerSnapshotOp(podUID, state.ContainerName, baseDir, c.snapshotter)
		if state.Completed {
			glog.V(4).Infof("Container %s has already been snapshotted", state.ContainerName)
			op.completed = true
		} else {
			glog.V(4).Infof("Container %s needs to be snapshotted", state.ContainerName)
		}
		c.operations[state.ContainerName] = op
	}

	for name, op := range c.operations {
		if op.clear {
			glog.V(4).Infof("Container %s was removed from disk", name)
			delete(c.operations, name)
		}
	}

	select {
	case c.work <- struct{}{}:
	default:
	}
	return nil
}

type containerSnapshotOp struct {
	fromContainer string
	podUID        string

	path      string
	clear     bool
	completed bool

	snapshotter ContainerSnapshotter
}

func newContainerSnapshotOp(podUID, fromContainer, path string, snapshotter ContainerSnapshotter) *containerSnapshotOp {
	return &containerSnapshotOp{
		fromContainer: fromContainer,
		podUID:        podUID,
		path:          path,
		snapshotter:   snapshotter,
	}
}

type SnapshotError struct {
	Status        string `json:"status"`
	FromContainer string `json:"fromContainer"`
	Message       string `json:"message"`
}

func (c *containerSnapshotOp) Run() error {
	statusPath := filepath.Join(c.path, c.fromContainer) + resultFileSuffix
	if err := os.Remove(statusPath); err != nil && !os.IsNotExist(err) {
		glog.Errorf("Unable to remove existing JSON status for client: %v", err)
	}

	result := &SnapshotError{
		Status:        "Success",
		FromContainer: c.fromContainer,
	}

	err := c.writeSnapshot()
	if err != nil {
		result.Status = "Failure"
		result.Message = err.Error()
	}

	data, jsonErr := json.MarshalIndent(result, "", "  ")
	if jsonErr != nil {
		glog.Errorf("Unable to write JSON status back to client: %v", err)
		return err
	}
	data = append(data, []byte("\n")...)
	if fileErr := ioutil.WriteFile(statusPath, data, 0660); fileErr != nil {
		glog.Errorf("Unable to write JSON status back to client: %v", fileErr)
		return err
	}
	return err
}

func (c *containerSnapshotOp) writeSnapshot() error {
	path := filepath.Join(c.path, c.fromContainer)
	glog.V(4).Infof("Writing snapshot to %s", path)

	var isNamedPipe bool
	err := func() error {
		in, err := c.snapshotter.Snapshot(c.podUID, c.fromContainer)
		if err != nil {
			return err
		}
		defer in.Close()

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		defer f.Close()

		// if this is a named pipe, we delete it afterwards
		fi, err := os.Stat(path)
		if err != nil {
			return err
		}
		isNamedPipe = fi.Mode()&os.ModeNamedPipe == os.ModeNamedPipe

		if _, err := io.Copy(f, in); err != nil {
			return err
		}
		return f.Close()
	}()

	// always delete the file on error
	if err != nil || isNamedPipe {
		if err := os.Remove(path); err != nil {
			glog.Errorf("Unable to remove snapshot path: %v", err)
		}
	}
	return err
}
