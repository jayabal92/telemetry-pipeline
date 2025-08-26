PROTO_DIR=proto

.PHONY: proto build run-producer run-consumer run-api-server

proto:
	protoc --go_out=paths=source_relative:. \
       --go-grpc_out=paths=source_relative:. \
       ${PROTO_DIR}/*.proto

build: proto
	go build ./...


run-producer:
	go run cmd/producer/main.go

run-consumer:
	go run cmd/consumer/main.go

run-api-server:
	go run cmd/api/main.go
