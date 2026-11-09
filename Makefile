.PHONY: run build test ctest lint benchmark tidy migrate-up
LOCAL_BIN := $(CURDIR)/bin
GOLANGCI_LINT ?= $(LOCAL_BIN)/golangci-lint

run:
	go run ./cmd/recommendations

build:
	CGO_ENABLED=0 go build ./...

test:
	go test -race ./...

ctest:
	@test -n "$${GOALS_TEST_DSN:-}" || (echo "GOALS_TEST_DSN must point to a dedicated test database"; exit 1)
	go test -race ./internal/goals -run TestPostgresGoalLifecycle -count=1

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; exit 1)

benchmark:
	go test ./internal/planner -run '^$$' -bench BenchmarkBuild -benchmem

tidy:
	go mod tidy

migrate-up:
	go run ./cmd/recommendations migrate up

$(GOLANGCI_LINT):
	GOBIN="$(LOCAL_BIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
