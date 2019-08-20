# Go parameters

DEBUG ?= 1

GOCMD = go
GOFLAG =
GOCLEAN = $(GOCMD) clean
GOGET = $(GOCMD) get

ifeq ($(DEBUG), 0)
	GOFLAG += -ldflags "-s -w"
endif

BINARY_NAME=elector
SRC_FILE = ./main.go

all: build

build:
	$(GOCMD) build $(GOFLAG) -o $(BINARY_NAME) $(SRC_FILE)

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

deps:
