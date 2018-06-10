container-snapshot
==================

This project is in early `alpha` and may change significantly in the
future.

## Trying it out

Clone the source into your GOPATH and build with:

    make

To test locally without a running Kubernetes server, start your Docker 
daemon and then run from the same host as the daemon:

    mkdir /tmp/fake-pod
    ./container-snapshot -v=5 --bind-local=/tmp/fake-pod --listen-csi= &

to begin listening for contents in the `/tmp/fake-pod` directory. You can start
a fake container with:

    make fake

creating some changes in the container filesystem:

    docker exec snapshot-test touch /myfile
    docker diff snapshot-test

and then creating an empty file in `/tmp/fake-pod` that matches the name of the
container:

    touch /tmp/fake-pod/sleep

The contents of that file will be populated with the delta of the container
filesystem:

    tar tvzf /tmp/fake-pod/sleep