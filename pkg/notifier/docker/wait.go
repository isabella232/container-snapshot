package docker

import (
	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/notifier"
)

func (n *dockerNotifier) Wait(condition notifier.ConditionType, podUID, containerName string) <-chan struct{} {
	// register for wait before we perform a live lookup, in case we race with our
	// event or sync loops
	ch := n.waitFor(condition, podUID, containerName)
	ready := func() bool {
		apiContainer, err := n.newestContainer(podUID, containerName)
		if err == errNoSuchContainer {
			return false
		}
		if err != nil {
			return true
		}
		return containerStateSatisfied(apiContainer.State, condition)
	}()
	if ready {
		n.waitFinished(podUID, containerName, ch)
		return nil
	}
	glog.V(5).Infof("Will wait for %s on %s in pod %s", condition, containerName, podUID)
	return ch
}

func containerStateSatisfied(state string, condition notifier.ConditionType) bool {
	glog.V(6).Infof("Checking container state %s", state)
	switch {
	case condition == notifier.ConditionNode:
		return true
	case condition == notifier.ConditionDone, condition == notifier.ConditionFailed, condition == notifier.ConditionSuccess:
		switch state {
		case "running", "created":
			return false
		default:
			return true
		}
	default:
		return true
	}
}

func (n *dockerNotifier) waitSync() {
	n.lock.Lock()
	defer n.lock.Unlock()

	// mark any running containers
	seenPodUIDs := make(map[string]struct{})
	for _, pod := range n.pods {
		podUID := pod.UID
		seenPodUIDs[podUID] = struct{}{}
		if wait, ok := n.waits[podUID]; ok {
			for i, c := range wait.containers {
				_, ok := pod.Containers[c.name]
				if !ok || containerStateSatisfied("running", c.condition) {
					if c.ch != nil {
						glog.V(4).Infof("Satisfied condition %q on %s in pod %s during sync", c.condition, c.name, podUID)
						close(c.ch)
						wait.containers[i].ch = nil
					}
				}
			}
		}
	}

	// remove any pods that have no visible containers and compact any completed containers
	for podUID, waits := range n.waits {
		if _, ok := seenPodUIDs[podUID]; ok {
			if waits.compact() {
				delete(n.waits, podUID)
			}
			continue
		}
		// pod not in list
		for _, c := range waits.containers {
			if c.ch != nil {
				glog.V(4).Infof("Satisfied condition %q on %s in pod %s during sync due to shutdown", c.condition, c.name, podUID)
				close(c.ch)
				c.ch = nil
			}
		}
		delete(n.waits, podUID)
	}
}

func (n *dockerNotifier) waitFor(condition notifier.ConditionType, podUID, containerName string) chan struct{} {
	n.lock.Lock()
	defer n.lock.Unlock()
	pod, ok := n.waits[podUID]
	if !ok {
		pod = &podWait{podUID: podUID}
		n.waits[podUID] = pod
	}
	ch := make(chan struct{})
	pod.containers = append(pod.containers, containerWait{
		name:      containerName,
		condition: condition,
		ch:        ch,
	})
	return ch
}

func (n *dockerNotifier) waitFinished(podUID, containerName string, ch chan struct{}) {
	n.lock.Lock()
	defer n.lock.Unlock()
	pod, ok := n.waits[podUID]
	if !ok {
		return
	}
	for i := range pod.containers {
		if pod.containers[i].ch == ch {
			close(ch)
			pod.containers[i].ch = nil
			pod.containers = append(pod.containers[:i], pod.containers[i+1:]...)
			return
		}
	}
}
