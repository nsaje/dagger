all:
	go install ./...

docker-scratch:
	gox -output="bin/{{.OS}}_{{.Arch}}/{{.Dir}}" -osarch="linux/amd64" ./...
	docker build -t dagger-scratch -f Dockerfile.scratch .

race:
	go install -race ./...

test: all
	go test
	
