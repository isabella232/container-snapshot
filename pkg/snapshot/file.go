package snapshot

import (
	"io"
	"os"
)

type snapshotFromFile struct {
	path string
}

func NewSnapshotFromFile(path string) ContainerSnapshotter {
	return &snapshotFromFile{path: path}
}

func (s *snapshotFromFile) Snapshot(podUID, containerName string) (io.ReadCloser, error) {
	return os.Open(s.path)
}
