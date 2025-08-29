PROTO_DIR   := proto
SWAG        := $(GOPATH)/bin/swag
PROJECT_NAME:= telemetry-pipeline
REGISTRY    ?= telemetry-pipeline
VERSION     ?= latest
NAMESPACE   ?= telemetry
CHART_NAME  := telemetry-platform
CHART_DIR   := chart/telemetry-platform
RELEASE_NAME := tp

# Images
MSG_QUEUE_IMG   := $(REGISTRY)/msg-queue:$(VERSION)
PRODUCER_IMG    := $(REGISTRY)/producer:$(VERSION)
CONSUMER_IMG    := $(REGISTRY)/consumer:$(VERSION)
APISERVER_IMG   := $(REGISTRY)/apiserver:$(VERSION)

# -------------------------------------------------------------------
# Code generation
# -------------------------------------------------------------------
.PHONY: proto swag openapi tidy build run-producer run-consumer run-apiserver
proto:
	protoc --go_out=paths=source_relative:. \
	       --go-grpc_out=paths=source_relative:. \
	       $(PROTO_DIR)/*.proto

swag:
	$(SWAG) init \
		-g cmd/api-server/main.go \
		-d . \
		-o api/docs

openapi: swag
	@echo "OpenAPI spec generated at ./api/docs/swagger.json and ./api/docs/swagger.yaml"

tidy:
	go mod tidy

build: proto
	go build ./...

run-producer:
	go run cmd/producer/main.go

run-consumer:
	go run cmd/consumer/main.go

run-apiserver:
	go run cmd/api-server/main.go

.PHONY: test
test:
	@echo ">>> Running unit tests..."
	@go test ./... -coverprofile=coverage.out -v
	@echo ">>> Coverage report:"
	@go tool cover -func=coverage.out | tail -n 10

cover:
	@go tool cover -html=coverage.out

# -------------------------------------------------------------------
# Docker builds
# -------------------------------------------------------------------
.PHONY: docker-all docker-msg-queue docker-producer docker-consumer docker-apiserver push-msg-queue push-producer push-consumer push-apiserver push-all minikube-load

docker-all: docker-msg-queue docker-producer docker-consumer docker-apiserver

docker-msg-queue:
	docker build -f Dockerfile.msg-queue -t $(MSG_QUEUE_IMG) .

docker-producer:
	docker build -f Dockerfile.producer -t $(PRODUCER_IMG) .

docker-consumer:
	docker build -f Dockerfile.consumer -t $(CONSUMER_IMG) .

docker-apiserver:
	docker build -f Dockerfile.apiserver -t $(APISERVER_IMG) .

push-msg-queue:
	docker push $(MSG_QUEUE_IMG)

push-producer:
	docker push $(PRODUCER_IMG)

push-consumer:
	docker push $(CONSUMER_IMG)

push-apiserver:
	docker push $(APISERVER_IMG)

push-all: push-msg-queue push-producer push-consumer push-apiserver

minikube-load:
	minikube image load $(MSG_QUEUE_IMG)
	minikube image load $(PRODUCER_IMG)
	minikube image load $(CONSUMER_IMG)
	minikube image load $(APISERVER_IMG)

# -------------------------------------------------------------------
# Helm (umbrella chart)
# -------------------------------------------------------------------
.PHONY: deps install upgrade template uninstall

deps:
	helm dependency update $(CHART_DIR)

install: deps
	helm install $(RELEASE_NAME) $(CHART_DIR) -n $(NAMESPACE) --create-namespace

upgrade: deps
	helm upgrade --install $(RELEASE_NAME) $(CHART_DIR) -n $(NAMESPACE)

template: deps
	helm template $(RELEASE_NAME) $(CHART_DIR) -n $(NAMESPACE)

uninstall:
	helm uninstall $(RELEASE_NAME) -n $(NAMESPACE)
