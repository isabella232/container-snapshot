package docker

import (
	"fmt"
	"io"
	"os"

	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/idtools"
	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/notifier"
)

func (n *dockerNotifier) Snapshot(condition notifier.ConditionType, podUID string, containerName string) (io.ReadCloser, error) {
	glog.V(4).Infof("Snapshotting container %s in pod %s", containerName, podUID)
	apiContainer, err := n.newestContainer(podUID, containerName)
	if err != nil {
		return nil, err
	}
	container, err := n.client.InspectContainer(apiContainer.ID)
	if err != nil {
		return nil, fmt.Errorf("no container %s in pod %s: %v", containerName, podUID, err)
	}

	switch condition {
	case notifier.ConditionNow:
	case notifier.ConditionDone:
		if container.State.Running {
			return nil, fmt.Errorf("container %s in pod %s is not done, possible wait error", containerName, podUID)
		}
	case notifier.ConditionFailed:
		if container.State.Running {
			return nil, fmt.Errorf("container %s in pod %s is not done, possible wait error for failure", containerName, podUID)
		}
		if container.State.ExitCode == 0 {
			return nil, fmt.Errorf("container %s in pod %s succeeded instead of failed", containerName, podUID)
		}
	case notifier.ConditionSuccess:
		if container.State.Running {
			return nil, fmt.Errorf("container %s in pod %s is not done, possible wait error for success", containerName, podUID)
		}
		if container.State.ExitCode != 0 {
			return nil, fmt.Errorf("container %s in pod %s failed (exit code %d)", containerName, podUID, container.State.ExitCode)
		}
	default:
		return nil, fmt.Errorf("unrecognized condition %s", condition)
	}

	// locate an overlay2 filesystem
	if container.GraphDriver == nil {
		return nil, fmt.Errorf("cannot snapshot container, container runtime does not implement snapshotting")
	}
	if container.GraphDriver.Name != "overlay2" {
		return nil, fmt.Errorf("cannot snapshot container, unsupported storage type %s", container.GraphDriver.Name)
	}
	path := container.GraphDriver.Data["UpperDir"]
	if len(path) == 0 {
		return nil, fmt.Errorf("cannot snapshot container, layer cannot be found")
	}

	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("unable to snapshot container, the specified layer directory could not be located - verify you are running on the same filesystem as the container runtime")
		}
		return nil, fmt.Errorf("unable to snapshot container, the specified layer directory could not be located: %v", err)
	}

	// TODO: calculate these from the docker daemon
	var uidMaps, gidMaps []idtools.IDMap

	// stream the upper dir
	glog.V(4).Infof("Streaming layer diff contents from %s", path)
	return archive.TarWithOptions(path, &archive.TarOptions{
		Compression:    archive.Gzip,
		UIDMaps:        uidMaps,
		GIDMaps:        gidMaps,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
	})
}
