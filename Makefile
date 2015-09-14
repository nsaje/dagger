all:
	gox -osarch="darwin/amd64" -output="bin/{{.Dir}}" ./...

race:
	gox -race -osarch="darwin/amd64" -output="bin/{{.Dir}}" ./...

test: all
	go test
	
