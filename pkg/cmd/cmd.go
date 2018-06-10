package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/MakeNowJust/heredoc"
	"github.com/spf13/cobra"

	"github.com/openshift/container-snapshot/pkg/snapshot"
)

// New provides a command that runs a container snapshot daemon.
func New(name string) *cobra.Command {
	server := &snapshot.Server{
		CSIStateDirectory: filepath.Join(os.TempDir(), "container-snapshot-state"),
	}
	if wd, err := os.Getwd(); err == nil {
		server.ListenCSIAddr = fmt.Sprintf("unix://%s", filepath.Join(wd, "csi.sock"))
	}
	cmd := &cobra.Command{
		Use:   name,
		Short: "Allow pods to easily snapshot their containers",
		Long: heredoc.Doc(`
			Start the container snapshot daemon

			This command launches a daemon that allows unprivileged containers in a Kubernetes pod
			to snapshot their contents. The daemon either runs as a CSI storage plugin or by
			directly watching the Docker daemon.

			When running with Docker, bind mount the /var/run/container-snapshot.openshift.io directory
			(writable) and then create an empty file in that directory that has the same name as
			a container in the pod. The daemon will write a gzipped and tarred Docker layer
			diff archive to that location. You may also create a named pipe with "mkfifo" and read
			from the pipe to stream contents without hitting disk.`),
		RunE: func(c *cobra.Command, args []string) error {
			return server.Start()
		},
	}
	cmd.Flags().StringVar(&server.CSIStateDirectory, "csi-state-directory", server.CSIStateDirectory, "The directory to record state for the CSI volume requests served by this plugin.")
	cmd.Flags().StringVar(&server.ListenCSIAddr, "listen-csi", server.ListenCSIAddr, "The Unix domain socket path to listen for CSI volume requests. If set to empty the server will not start.")
	cmd.Flags().StringVar(&server.MountDirectory, "bind-local", server.MountDirectory, "When set, servers will watch this directory for any created pod instead of the pod's directory.")
	cmd.Flags().StringVar(&server.SnapshotFrom, "snapshot-file", server.SnapshotFrom, "Serve the provided file instead of snapshotting the container. For testing only.")
	return cmd
}
