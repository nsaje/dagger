all:
	gox -osarch="darwin/amd64" -output="{{.Dir}}" ./...