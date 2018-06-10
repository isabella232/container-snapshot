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
	running     chan string
	lock        sync.Mutex
	operations  map[string]*containerSnapshotOp
}

func newContainerSnapshotTracker(snapshotter notifier.ContainerSnapshotter) *containerSnapshotTracker {
	return &containerSnapshotTracker{
		snapshotter: snapshotter,
		pending:     make(chan containerConditionReached, 5),
		work:        make(chan struct{}, 1),
		running:     make(chan string, 2),
		operations:  make(map[string]*containerSnapshotOp),
	}
}

// Run attempts to process the operations queue until stopCh is closed. It is safe to run
// multiple snapshot trackers in parallel.
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
				op := c.pop()
				if op == nil {
					glog.V(5).Infof("No work")
					break
				}
				if ch := c.snapshotter.Wait(op.condition, op.podUID, op.containerName); ch != nil {
					c.wait(op, ch)
					glog.V(5).Infof("Deferred %s in pod %s due to unsatisfied condition", op.containerName, op.podUID)
					continue
				}
				if err := c.run(op); err != nil {
					glog.Errorf(err.Error())
				}
			}
		}
	}
}

// pop takes one snapshot op off the queue if it is not completed, not pending,
// and not already running. It will return nil if the queue is empty
func (c *containerSnapshotTracker) pop() *containerSnapshotOp {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, op := range c.operations {
		if !op.completed && !op.running && op.pending == nil {
			op.running = true
			return op
		}
	}
	return nil
}

// run executes a snapshot operation, updating the op with the correct
// state after it succeeds.
func (c *containerSnapshotTracker) run(op *containerSnapshotOp) error {
	// make sure someone doesn't accidentally invoke us with an op that is
	// not running
	c.lock.Lock()
	if !op.running {
		panic("snapshot op must be marked as running before calling run()")
	}
	c.lock.Unlock()

	// ensure the operation is marked complete
	defer func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		op.running = false
		op.completed = true
	}()

	return op.Run()
}

// wait sets the operation aside (not in the queue) until the provided channel is closed.
func (c *containerSnapshotTracker) wait(op *containerSnapshotOp, ch <-chan struct{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if op.completed {
		panic("should never call wait on a completed operation")
	}
	op.running = false
	op.pending = ch

	go func() {
		<-ch
		c.pending <- containerConditionReached{PodUID: op.podUID, ContainerName: op.containerName}
	}()
}

// ready indicates that a given container should appear in the queue.
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
			if op.running {
				op.clear = false
				continue
			}
			if state.Completed {
				glog.V(4).Infof("Container %s set to completed", state.ContainerName)
				op.completed = state.Completed
			}
			op.clear = false
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
		if !op.clear {
			continue
		}
		if op.running {
			glog.V(4).Infof("Container %s was removed from disk but is still running", name)
			continue
		}
		glog.V(4).Infof("Container %s was removed from disk", name)
		delete(c.operations, name)
	}

	select {
	case c.work <- struct{}{}:
	default:
	}
	return nil
}
