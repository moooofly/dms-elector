all:
	@# generate .pb.go based on .proto
	protoc --go_out=plugins=grpc:. *.proto

clean:
	rm -f *.pb.go
