package snapshot

import (
	"io"
	"os"

	"github.com/openshift/container-snapshot/pkg/notifier"
)

type snapshotFromFile struct {
	path string
}

func NewSnapshotFromFile(path string) notifier.ContainerSnapshotter {
	return &snapshotFromFile{path: path}
}

func (s *snapshotFromFile) Snapshot(condition notifier.ConditionType, podUID, containerName string) (io.ReadCloser, error) {
	return os.Open(s.path)
}

func (s *snapshotFromFile) Wait(condition notifier.ConditionType, podUID, containerName string) <-chan struct{} {
	return nil
}
