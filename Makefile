all:
	go install ./...

docker-scratch:
	gox -output="bin/{{.OS}}_{{.Arch}}/{{.Dir}}" -osarch="linux/amd64" ./...
	docker build -t dagger-scratch -f Dockerfile.scratch .

race:
	go install -race ./...

test: all
	go test
	
mocks:
	rm -f dagger/mocks.go
	mockgen -self_package="dagger" -package="dagger" github.com/nsaje/dagger/dagger Coordinator,InputManager,TaskStarter,Task >_mocks.go
	sed -i.bak 's/dagger\.//g' _mocks.go
	rm _mocks.go.bak
	mv _mocks.go dagger/mocks.go
	goimports -w dagger/mocks.go
