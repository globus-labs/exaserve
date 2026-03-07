BINARY  := bin/go_dispatch
PKG     := .
GOFLAGS := -trimpath

.PHONY: all build linux clean

all: build

build:
	@mkdir -p bin
	go build $(GOFLAGS) -o $(BINARY) $(PKG)

# Cross-compile for HPC (Linux/amd64) from any host
linux:
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -o $(BINARY) $(PKG)

clean:
	rm -rf bin
