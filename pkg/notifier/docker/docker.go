package docker

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	docker "github.com/fsouza/go-dockerclient"
	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/notifier"
)

type namespacedName struct {
	namespace string
	name      string
}

type podInfo struct {
	Namespace  string
	Name       string
	UID        string
	Containers map[string]*notifier.ContainerInfo
}

type podWait struct {
	podUID     string
	containers []containerWait
}

func (w *podWait) compact() bool {
	for i := 0; i < len(w.containers); {
		if w.containers[i].ch != nil {
			i++
			continue
		}
		for j := i + 1; j < len(w.containers); j = i + 1 {
			w.containers[i] = w.containers[j]
			if w.containers[i].ch != nil {
				i++
			}
		}
		if w.containers[i].ch == nil {
			w.containers = w.containers[:i]
			break
		}
	}
	return len(w.containers) == 0
}

type containerWait struct {
	name      string
	condition notifier.ConditionType
	ch        chan struct{}
}

// dockerNotifier watches Docker events from the daemon, attempting to find containers that
//
//   1. Were created by Kubernetes and have the appropriate metadata
//   2. Have a directory mount volume at /var/run/container-snapshot.openshift.io that is read-write
//
// and then invokes the Container notifier with info about the created container. It guarantees
// a single container is sent.
//
// TODO: could be replaced by a FlexVolume or CSI.
// TODO: periodically check the mount paths for all pods and if they have been deleted, fire
//       a notification
type dockerNotifier struct {
	client       *docker.Client
	notifier     notifier.Containers
	pods         map[namespacedName]*podInfo
	syncInterval time.Duration

	lock  sync.Mutex
	waits map[string]*podWait
}

func New(client *docker.Client, n notifier.Containers) *dockerNotifier {
	return &dockerNotifier{
		client:       client,
		notifier:     n,
		pods:         make(map[namespacedName]*podInfo),
		syncInterval: time.Minute / 4,

		waits: make(map[string]*podWait),
	}
}

func (n *dockerNotifier) Run(stopCh <-chan struct{}) error {
	eventsCh := make(chan *docker.APIEvents, 1000)
	if err := n.client.AddEventListener(eventsCh); err != nil {
		return err
	}
	firstCh := make(chan time.Time, 1)
	firstCh <- time.Time{}
	var timeCh <-chan time.Time = firstCh
	var lastCreatedID string
	go func() {
		defer glog.Infof("Exiting event loop")
		for {
			select {
			case <-stopCh:
				break
			case <-timeCh:
				if timeCh == firstCh {
					timeCh = time.NewTicker(n.syncInterval).C
				}
				containers, err := n.client.ListContainers(docker.ListContainersOptions{Before: lastCreatedID})
				if err != nil {
					glog.Errorf("Unable to list containers: %v", err)
					break
				}

				newPods := make(map[namespacedName]*podInfo)
				newestContainer := int64(0)
				for i := range containers {
					info := kubernetesInfoForMap(containers[i].Labels)
					info.ContainerID = containers[i].ID
					info.Created = containers[i].Created
					if info.Created > newestContainer {
						newestContainer = info.Created
					}
					n.containerCreated(newPods, info, &containers[i])
				}
				if glog.V(6) {
					glog.Infof("Sync:\nOld pods: %s\nNew pods: %s", spew.Sdump(n.pods), spew.Sdump(newPods))
				} else {
					glog.V(4).Infof("Periodic sync: %d pods", len(newPods))
				}
				for k, oldPod := range n.pods {
					newPod, ok := newPods[k]
					if !ok {
						// the entire pod has been removed, remove all containers
						for _, oldContainer := range oldPod.Containers {
							n.notifier.MountRemoved(oldContainer.Copy())
						}
						continue
					}
					// update any containers where mount path or pod UID changed
					for name, oldContainer := range oldPod.Containers {
						if newContainer, ok := newPod.Containers[name]; ok {
							if newContainer.MountPath == oldContainer.MountPath && newContainer.PodUID == oldContainer.PodUID {
								continue
							}
							glog.V(4).Infof("Refresh pod %s/%s container %s (%s -> %s)", newContainer.PodNamespace, newContainer.PodName, newContainer.ContainerName, oldContainer.MountPath, newContainer.MountPath)
							n.notifier.MountRemoved(oldContainer.Copy())
							n.notifier.MountAdded(newContainer.Copy())
						} else {
							n.notifier.MountRemoved(oldContainer.Copy())
						}
					}
					// notify for all newly added containers to existing pods
					for name, newContainer := range newPod.Containers {
						if _, ok := oldPod.Containers[name]; !ok {
							n.notifier.MountAdded(newContainer.Copy())
						}
					}
				}
				// notify for all newly created pods
				for k, newPod := range newPods {
					if _, ok := n.pods[k]; ok {
						continue
					}
					for _, newContainer := range newPod.Containers {
						n.notifier.MountAdded(newContainer.Copy())
					}
				}
				n.pods = newPods

				n.waitSync()

				allMounts := make([]*notifier.ContainerInfo, 0, len(containers))
				for _, newPod := range newPods {
					for _, newContainer := range newPod.Containers {
						allMounts = append(allMounts, newContainer.Copy())
					}
				}
				sort.SliceStable(allMounts, func(i, j int) bool {
					a, b := allMounts[i], allMounts[j]
					if a.PodUID < b.PodUID {
						return true
					}
					if a.PodUID > b.PodUID {
						return false
					}
					if a.ContainerID < b.ContainerID {
						return true
					}
					if a.ContainerID > b.ContainerID {
						return false
					}
					return false
				})
				n.notifier.MountSync(allMounts)

			case event, ok := <-eventsCh:
				if !ok {
					break
				}
				switch event.Type {
				case "container":
					switch event.Action {
					case "create":
						lastCreatedID = event.Actor.ID
						info := kubernetesInfoForMap(event.Actor.Attributes)
						info.ContainerID = event.Actor.ID
						info.Created = event.Time
						added, removed := n.containerCreated(n.pods, info, nil)
						for _, remove := range removed {
							n.notifier.MountRemoved(remove)
						}
						if added {
							n.notifier.MountAdded(info)
						}
					}
				}
			}
		}
	}()
	return nil
}

var errNoSuchContainer = fmt.Errorf("no container found in that pod")

func (n *dockerNotifier) newestContainer(podUID, containerName string) (*docker.APIContainers, error) {
	containers, err := n.client.ListContainers(docker.ListContainersOptions{All: true, Filters: map[string][]string{
		"label": []string{
			fmt.Sprintf("io.kubernetes.container.name=%s", containerName),
			fmt.Sprintf("io.kubernetes.pod.uid=%s", podUID),
		},
	}})
	if err != nil {
		return nil, fmt.Errorf("unable to find container %s in pod %s: %v", containerName, podUID, err)
	}

	if len(containers) == 0 {
		return nil, errNoSuchContainer
	}

	// pick the newest
	return newestAPIContainer(containers), nil
}

func kubernetesInfoForMap(attrs map[string]string) *notifier.ContainerInfo {
	return &notifier.ContainerInfo{
		PodUID:        attrs["io.kubernetes.pod.uid"],
		PodNamespace:  attrs["io.kubernetes.pod.namespace"],
		PodName:       attrs["io.kubernetes.pod.name"],
		ContainerName: attrs["io.kubernetes.container.name"],
	}
}

func (n *dockerNotifier) containerCreated(pods map[namespacedName]*podInfo, info *notifier.ContainerInfo, containerInfo *docker.APIContainers) (added bool, removed []*notifier.ContainerInfo) {
	// ignore containers that don't expose Kubernetes metadata
	if len(info.PodUID) == 0 {
		return false, removed
	}
	existing, ok := pods[namespacedName{namespace: info.PodNamespace, name: info.PodName}]
	if ok {
		if existing.UID == info.PodUID {
			// we already have seen this container and we're an older container, no need to check again
			if oldContainer, ok := existing.Containers[info.ContainerName]; ok {
				if info.Created <= oldContainer.Created {
					return false, removed
				}
			}
		} else {
			// assume we're seeing a new pod with the same name, all old containers should be removed
			if info.Created > newestContainer(existing.Containers) {
				for _, oldContainer := range existing.Containers {
					removed = append(removed, oldContainer)
				}
				existing = nil
			}
		}
	}

	// fetch the mount list if necessary
	var mount string
	if containerInfo == nil {
		container, err := n.client.InspectContainer(info.ContainerID)
		if err != nil {
			if _, ok := err.(*docker.NoSuchContainer); !ok {
				glog.Errorf("Unable to find container %q that was delivered via event: %v", info.ContainerID, err)
			}
			return false, removed
		}
		mount, ok = findKubernetesMountDir(container)
	} else {
		mount, ok = findKubernetesAPIMountDir(containerInfo)
	}
	if !ok {
		return false, removed
	}
	info.MountPath = mount

	if existing == nil {
		existing = &podInfo{
			Namespace:  info.PodNamespace,
			Name:       info.PodName,
			UID:        info.PodUID,
			Containers: make(map[string]*notifier.ContainerInfo),
		}
		pods[namespacedName{namespace: info.PodNamespace, name: info.PodName}] = existing
	}

	for _, existingContainer := range existing.Containers {
		if existingContainer.MountPath == mount {
			// a container already has mounted this path, no need to add another
			return false, removed
		}
	}

	copied := *info
	existing.Containers[info.ContainerName] = &copied
	return true, removed
}

func findKubernetesMountDir(container *docker.Container) (path string, ok bool) {
	for _, mount := range container.Mounts {
		if mount.Destination == "/var/run/container-snapshot.openshift.io" && mount.RW {
			return mount.Source, true
		}
	}
	return "", false
}

func findKubernetesAPIMountDir(container *docker.APIContainers) (path string, ok bool) {
	for _, mount := range container.Mounts {
		if mount.Destination == "/var/run/container-snapshot.openshift.io" && mount.RW {
			return mount.Source, true
		}
	}
	return "", false
}

func newestContainer(containers map[string]*notifier.ContainerInfo) int64 {
	newest := int64(0)
	for _, container := range containers {
		if container != nil && container.Created > newest {
			newest = container.Created
		}
	}
	return newest
}

func newestAPIContainer(containers []docker.APIContainers) *docker.APIContainers {
	t := int64(0)
	var newest *docker.APIContainers
	for i, container := range containers {
		if container.Created >= t {
			t = container.Created
			newest = &containers[i]
		}
	}
	return newest
}
