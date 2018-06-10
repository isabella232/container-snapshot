package snapshot

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/golang/glog"

	"github.com/openshift/container-snapshot/pkg/notifier"
)

type containerSnapshotOp struct {
	snapshotter   notifier.ContainerSnapshotter
	containerName string
	podUID        string
	filename      string
	path          string
	condition     notifier.ConditionType

	// only used by the tracker while under its lock
	clear     bool
	running   bool
	completed bool
	pending   <-chan struct{}
}

func newContainerSnapshotOp(snapshotter notifier.ContainerSnapshotter, podUID, containerName, filename, path string, condition notifier.ConditionType) *containerSnapshotOp {
	return &containerSnapshotOp{
		snapshotter:   snapshotter,
		podUID:        podUID,
		containerName: containerName,
		filename:      filename,
		path:          path,
		condition:     condition,
	}
}

type SnapshotError struct {
	Status        string `json:"status"`
	FromContainer string `json:"containerName"`
	Message       string `json:"message"`
}

func (c *containerSnapshotOp) Run() error {
	statusPath := filepath.Join(c.path, c.containerName) + resultFileSuffix
	if err := os.Remove(statusPath); err != nil && !os.IsNotExist(err) {
		glog.Errorf("Unable to remove existing JSON status for client: %v", err)
	}

	result := &SnapshotError{
		Status:        "Success",
		FromContainer: c.containerName,
	}

	err := c.writeSnapshot()
	if err != nil {
		result.Status = "Failure"
		result.Message = err.Error()
	}

	data, jsonErr := json.MarshalIndent(result, "", "  ")
	if jsonErr != nil {
		glog.Errorf("Unable to write JSON status back to client: %v", err)
		return err
	}
	data = append(data, []byte("\n")...)
	if fileErr := ioutil.WriteFile(statusPath, data, 0660); fileErr != nil {
		glog.Errorf("Unable to write JSON status back to client: %v", fileErr)
		return err
	}
	return err
}

func (c *containerSnapshotOp) writeSnapshot() error {
	path := filepath.Join(c.path, c.filename)
	glog.V(4).Infof("Writing snapshot to %s", path)

	var isNamedPipe bool
	err := func() error {
		in, err := c.snapshotter.Snapshot(c.condition, c.podUID, c.containerName)
		if err != nil {
			return err
		}
		defer in.Close()

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		defer f.Close()

		// if this is a named pipe, we delete it afterwards
		fi, err := os.Stat(path)
		if err != nil {
			return err
		}
		isNamedPipe = fi.Mode()&os.ModeNamedPipe == os.ModeNamedPipe

		if _, err := io.Copy(f, in); err != nil {
			return err
		}
		return f.Close()
	}()

	// always delete the file on error
	if err != nil || isNamedPipe {
		if err := os.Remove(path); err != nil {
			glog.Errorf("Unable to remove snapshot path: %v", err)
		}
	}
	return err
}
