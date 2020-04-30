GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GOOS       := $(shell go env GOOS)
GOARCH     := $(shell go env GOARCH)
PKGS       := $(shell go list ./... | grep -v vendor)


# NOTE: '-race' requires cgo; enable cgo by setting CGO_ENABLED=1
#BUILD_FLAG := -race
GOBUILD    := CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} go build $(BUILD_FLAG)

LDFLAGS += -X "github.com/moooofly/dms-elector/pkg/version.ClientVersion=$(shell cat VERSION)"
LDFLAGS += -X "github.com/moooofly/dms-elector/pkg/version.GoVersion=$(shell go version)"
LDFLAGS += -X "github.com/moooofly/dms-elector/pkg/version.UTCBuildTime=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/moooofly/dms-elector/pkg/version.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/moooofly/dms-elector/pkg/version.GitTag=$(shell git describe --tags)"
LDFLAGS += -X "github.com/moooofly/dms-elector/pkg/version.GitHash=$(shell git rev-parse HEAD)"

BINARY := dms-elector

.PHONY: all build install lint test pack docker misspell shellcheck clean

all: lint build test

build:
	@echo "==> Building ..."
	$(GOBUILD) -o $(BINARY) -ldflags '$(LDFLAGS)' .
	@echo ""

install:
	@echo "==> Installing ..."
	go install -ldflags '$(LDFLAGS)' .
	@echo ""

lint:
	@# gometalinter - Concurrently run Go lint tools and normalise their output
	@# - go get -u github.com/alecthomas/gometalinter  (Install from HEAD)
	@# - gometalinter --install  (Install all known linters)
	@echo "==> Running gometalinter ..."
	gometalinter --exclude=vendor --disable-all --enable=golint --enable=vet --enable=gofmt --enable=misspell
	find . -name '*.go' -not -path "./vendor/*" | xargs gofmt -w -s
	@echo ""

test:
	@echo "==> Testing ..."
	go test -short -race $(PKGS)
	@echo ""

deps:
	@echo "===> Tidy Dependencies ..."
	go mod tidy && go mod vendor
	@echo ""

build_x86:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(BUILD_FLAG) -o $(BINARY) -ldflags '$(LDFLAGS)' .

build_arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(BUILD_FLAG) -o $(BINARY) -ldflags '$(LDFLAGS)' .

build_darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(BUILD_FLAG) -o $(BINARY)_darwin_amd64 -ldflags '$(LDFLAGS)' .

pack: build_x86 build_arm64 build_darwin
	@echo "==> Packing ..."
	@tar czvf $(BINARY)-$(shell cat VERSION).linux-amd64.tar.gz $(BINARY)_linux_amd64 conf/*.conf
	@echo ""
	@tar czvf $(BINARY)-$(shell cat VERSION).linux-arm64.tar.gz $(BINARY)_linux_arm64 conf/*.conf
	@echo ""
	@tar czvf $(BINARY)-$(shell cat VERSION).darwin-amd64.tar.gz $(BINARY)_darwin_amd64 conf/*.conf
	@echo ""
	@rm $(BINARY)_linux_amd64
	@rm $(BINARY)_linux_arm64
	@rm $(BINARY)_darwin_amd64

misspell:
	@# misspell - Correct commonly misspelled English words in source files
	@#    go get -u github.com/client9/misspell/cmd/misspell
	@echo "==> Runnig misspell ..."
	find . -name '*.go' -not -path './vendor/*' -not -path './_repos/*' | xargs misspell -error
	@echo ""

clean:
	@echo "==> Cleaning ..."
	go clean -x -i .
	rm -f $(BINARY) $(BINARY)_*
	rm -rf *.out
	rm -rf *.tar.gz
	rm -f *.prof
	@echo ""

