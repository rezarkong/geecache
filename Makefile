SHELL := /bin/bash

.PHONY: test race bench bench-lru bench-lfu bench-lruk bench-arc demo-node1 demo-node2 demo-node3 demo-api demo-api-lfu demo-api-lruk demo-api-arc loadtest-hot loadtest-miss tidy

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test ./test/geecache -run '^$$' -bench . -benchmem

bench-lru:
	BENCH_EVICTOR=lru go test ./test/geecache -run '^$$' -bench . -benchmem

bench-lfu:
	BENCH_EVICTOR=lfu go test ./test/geecache -run '^$$' -bench . -benchmem

bench-lruk:
	BENCH_EVICTOR=lru-k go test ./test/geecache -run '^$$' -bench . -benchmem

bench-arc:
	BENCH_EVICTOR=arc go test ./test/geecache -run '^$$' -bench . -benchmem

tidy:
	go mod tidy

demo-node1:
	@echo "demo-node1 uses localhost:8001 and conflicts with demo-api/demo-api-lfu; use one or the other"
	go run ./cmd/server -addr=localhost:8001 -peers=localhost:8001,localhost:8002,localhost:8003

demo-node2:
	go run ./cmd/server -addr=localhost:8002 -peers=localhost:8001,localhost:8002,localhost:8003

demo-node3:
	go run ./cmd/server -addr=localhost:8003 -peers=localhost:8001,localhost:8002,localhost:8003

demo-api:
	@echo "demo-api replaces demo-node1; start demo-node2 and demo-node3 separately"
	go run ./cmd/server \
		-addr=localhost:8001 \
		-peers=localhost:8001,localhost:8002,localhost:8003 \
		-api=true \
		-api-addr=localhost:9999 \
		-empty-ttl=30s \
		-peer-retries=1

demo-api-lfu:
	@echo "demo-api-lfu replaces demo-node1; start demo-node2 and demo-node3 separately"
	go run ./cmd/server \
		-addr=localhost:8001 \
		-peers=localhost:8001,localhost:8002,localhost:8003 \
		-api=true \
		-api-addr=localhost:9999 \
		-empty-ttl=30s \
		-peer-retries=1 \
		-evictor=lfu

demo-api-lruk:
	@echo "demo-api-lruk replaces demo-node1; start demo-node2 and demo-node3 separately"
	go run ./cmd/server \
		-addr=localhost:8001 \
		-peers=localhost:8001,localhost:8002,localhost:8003 \
		-api=true \
		-api-addr=localhost:9999 \
		-empty-ttl=30s \
		-peer-retries=1 \
		-evictor=lru-k

demo-api-arc:
	@echo "demo-api-arc replaces demo-node1; start demo-node2 and demo-node3 separately"
	go run ./cmd/server \
		-addr=localhost:8001 \
		-peers=localhost:8001,localhost:8002,localhost:8003 \
		-api=true \
		-api-addr=localhost:9999 \
		-empty-ttl=30s \
		-peer-retries=1 \
		-evictor=arc

loadtest-hot:
	bash ./scripts/loadtest.sh "http://localhost:9999/api?key=Tom" 1000 50

loadtest-miss:
	bash ./scripts/loadtest.sh "http://localhost:9999/api?key=unknown" 1000 50
