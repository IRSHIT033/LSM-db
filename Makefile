.PHONY: build run clean

build:
	go mod tidy
	go build -o lsmdb ./cmd/cli

run: build
	./lsmdb

clean:
	rm -f lsmdb
	rm -rf data 