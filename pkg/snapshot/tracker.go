package snapshot

import (
	"fmt"
	"sync"

	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/notifier"
)

type State struct {
	ContainerName string
	Filename      string
	Completed     bool
	Pipe          bool
	Condition     notifier.ConditionType
}

type containerConditionReached struct {
	PodUID        string
	ContainerName string
}

type containerSnapshotTracker struct {
	snapshotter notifier.ContainerSnapshotter
	pending     chan containerConditionReached
	work        chan struct{}
	lock        sync.Mutex
	operations  map[string]*containerSnapshotOp
}

func newContainerSnapshotTracker(snapshotter notifier.ContainerSnapshotter) *containerSnapshotTracker {
	return &containerSnapshotTracker{
		snapshotter: snapshotter,
		pending:     make(chan containerConditionReached, 10),
		work:        make(chan struct{}, 1),
		operations:  make(map[string]*containerSnapshotOp),
	}
}

func (c *containerSnapshotTracker) Run(stopCh <-chan struct{}) {
	defer func() {
		glog.V(4).Infof("Snapshot tracker exiting")
	}()
	for {
		select {
		case <-stopCh:
			return
		case evt := <-c.pending:
			c.ready(evt.PodUID, evt.ContainerName)
		case <-c.work:
			for {
				op := c.next()
				if op == nil {
					glog.V(5).Infof("No work")
					break
				}
				if ch := c.snapshotter.Wait(op.condition, op.podUID, op.fromContainer); ch != nil {
					c.deferred(op, ch)
					glog.V(5).Infof("Deferred %s in pod %s due to unsatisfied condition", op.fromContainer, op.podUID)
					continue
				}
				if err := c.runOnce(op); err != nil {
					glog.Errorf(err.Error())
				}
			}
		}
	}
}

func (c *containerSnapshotTracker) runOnce(op *containerSnapshotOp) error {
	defer func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		op.completed = true
	}()
	return op.Run()
}

func (c *containerSnapshotTracker) deferred(op *containerSnapshotOp, ch <-chan struct{}) {
	go func() {
		<-ch
		c.pending <- containerConditionReached{PodUID: op.podUID, ContainerName: op.fromContainer}
	}()

	c.lock.Lock()
	defer c.lock.Unlock()
	op.pending = ch
}

func (c *containerSnapshotTracker) next() *containerSnapshotOp {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, op := range c.operations {
		if !op.completed && op.pending == nil {
			return op
		}
	}
	return nil
}

func (c *containerSnapshotTracker) ready(podUID, containerName string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if op, ok := c.operations[containerName]; ok {
		if podUID != op.podUID {
			panic(fmt.Sprintf("got unexpected podUID to this tracker (want %s, got %s)", podUID, op.podUID))
		}
		op.pending = nil
	}
	select {
	case c.work <- struct{}{}:
	default:
	}
}

func (c *containerSnapshotTracker) Sync(states []State, podUID, baseDir string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, op := range c.operations {
		op.clear = true
	}

	for _, state := range states {
		if op, ok := c.operations[state.ContainerName]; ok {
			glog.V(4).Infof("Container %s set to completed", state.ContainerName)
			op.clear = false
			op.completed = state.Completed
			continue
		}
		op := newContainerSnapshotOp(c.snapshotter, podUID, state.ContainerName, state.Filename, baseDir, state.Condition)
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
