package notifier

import (
	"io"
)

type ContainerInfo struct {
	MountPath string

	PodUID        string
	PodNamespace  string
	PodName       string
	ContainerName string
	ContainerID   string

	Created int64
}

func (c *ContainerInfo) Copy() *ContainerInfo {
	copied := *c
	return &copied
}

type Containers interface {
	MountSync(infos []*ContainerInfo)
	MountAdded(info *ContainerInfo)
	MountRemoved(info *ContainerInfo)
}

type ConditionType string

const (
	ConditionNode    ConditionType = "now"
	ConditionSuccess ConditionType = "success"
	ConditionFailed  ConditionType = "failed"
	ConditionDone    ConditionType = "done"
)

type ContainerSnapshotter interface {
	Snapshot(condition ConditionType, podUID, containerName string) (io.ReadCloser, error)
	Wait(condition ConditionType, podUID, containerName string) <-chan struct{}
}
