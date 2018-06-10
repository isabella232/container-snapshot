FROM openshift/origin-release:golang-1.9
WORKDIR /go/src/github.com/openshift/container-snapshot
COPY . .
RUN go build ./cmd/container-snapshot

FROM centos:7
COPY --from=0 /go/src/github.com/openshift/container-snapshot/container-snapshot /usr/bin/