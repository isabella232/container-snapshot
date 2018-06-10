package snapshot

import (
	docker "github.com/fsouza/go-dockerclient"
)

// snapshotFilter selects mount directories in pod containers that are read-write and have the
// mount destination /var/run/container-snapshot.openshift.io
type snapshotFilter struct{}

func (_ snapshotFilter) IncludeContainer(container *docker.Container) (path string, ok bool) {
	for _, mount := range container.Mounts {
		if mount.Destination == "/var/run/container-snapshot.openshift.io" && mount.RW {
			return mount.Source, true
		}
	}
	return "", false
}

func (_ snapshotFilter) IncludeAPIContainer(container *docker.APIContainers) (path string, ok bool) {
	for _, mount := range container.Mounts {
		if mount.Destination == "/var/run/container-snapshot.openshift.io" && mount.RW {
			return mount.Source, true
		}
	}
	return "", false
}
