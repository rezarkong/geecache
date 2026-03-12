SHELL := /bin/bash

.PHONY: test race bench bench-lru bench-lfu demo-node1 demo-node2 demo-node3 demo-api loadtest-hot loadtest-miss tidy

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -bench . -benchmem

bench-lru:
	BENCH_EVICTOR=lru go test -bench . -benchmem

bench-lfu:
	BENCH_EVICTOR=lfu go test -bench . -benchmem

tidy:
	go mod tidy

demo-node1:
	go run ./cmd/server -addr=localhost:8001 -peers=localhost:8001,localhost:8002,localhost:8003

demo-node2:
	go run ./cmd/server -addr=localhost:8002 -peers=localhost:8001,localhost:8002,localhost:8003

demo-node3:
	go run ./cmd/server -addr=localhost:8003 -peers=localhost:8001,localhost:8002,localhost:8003

demo-api:
	go run ./cmd/server \
		-addr=localhost:8001 \
		-peers=localhost:8001,localhost:8002,localhost:8003 \
		-api=true \
		-api-addr=localhost:9999 \
		-empty-ttl=30s \
		-peer-retries=1

demo-api-lfu:
	go run ./cmd/server \
		-addr=localhost:8001 \
		-peers=localhost:8001,localhost:8002,localhost:8003 \
		-api=true \
		-api-addr=localhost:9999 \
		-empty-ttl=30s \
		-peer-retries=1 \
		-evictor=lfu

loadtest-hot:
	./scripts/loadtest.sh "http://localhost:9999/api?key=Tom" 1000 50

loadtest-miss:
	./scripts/loadtest.sh "http://localhost:9999/api?key=unknown" 1000 50
