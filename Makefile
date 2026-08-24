.PHONY: proto test

proto:
	PATH="$$PATH:$$(go env GOPATH)/bin" protoc \
		--go_out=. --go_opt=module=github.com/czq/cd-raft \
		--go-grpc_out=. --go-grpc_opt=module=github.com/czq/cd-raft \
		proto/cdraft.proto

test:
	go test ./...
