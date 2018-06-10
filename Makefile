
build:
	go build ./cmd/container-snapshot
.PHONY: build

check:
	go test ./...
.PHONY: check

deps:
	glide update -v --skip-test
.PHONY: deps

fake:
	-@docker stop snapshot-test
	-@docker rm snapshot-test
	docker run --name snapshot-test -d -v /var/run/container-snapshot.openshift.io/ \
		-l io.kubernetes.pod.uid=123 \
		-l io.kubernetes.pod.namespace=test \
		-l io.kubernetes.pod.name=daemon \
		-l io.kubernetes.container.name=sleep \
		--cgroup-parent system.slice \
		centos:7 /bin/bash -c 'exec sleep 10000'
.PHONY: fake
