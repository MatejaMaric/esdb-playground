all: build

test:
	go test -v ./...

build: test
	go build

run: build
	./esdb-playground

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func coverage.out
	rm coverage.out
