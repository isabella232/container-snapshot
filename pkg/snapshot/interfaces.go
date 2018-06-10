package snapshot

import (
	"io"
)

type ContainerSnapshotter interface {
	Snapshot(podUID string, containerName string) (io.ReadCloser, error)
}
