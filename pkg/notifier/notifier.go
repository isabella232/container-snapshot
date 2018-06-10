package notifier

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
