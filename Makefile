PROTO_DIR=proto
SWAG := $(GOPATH)/bin/swag


.PHONY: proto
proto:
	protoc --go_out=paths=source_relative:. \
       --go-grpc_out=paths=source_relative:. \
       ${PROTO_DIR}/*.proto

.PHONY: swag
swag:
	$(SWAG) init \
		-g cmd/api/main.go \
		-d . \
		-o api/docs

.PHONY: openapi
openapi: swag
	@echo "OpenAPI spec generated at ./api/docs/swagger.json and ./api/docs/swagger.yaml"

.PHONY: build
build: proto
	go build ./...

.PHONY: run-producer
run-producer:
	go run cmd/producer/main.go

.PHONY: run-consumer
run-consumer:
	go run cmd/consumer/main.go

.PHONY: run-api-server
run-api-server:
	go run cmd/api/main.go

.PHONY: tidy
tidy:
	go mod tidy
