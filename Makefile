.PHONY: run build test ctest resource-ctest diagnostic-ctest evidence-ctest activity-ctest lint benchmark tidy migrate-up
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
	go test -race ./internal/learnergoals -run TestPostgres -count=1
	go test -race ./internal/httpapi -run TestPostgresOnboarding -count=1
	go test -race ./internal/projection/catalog -run TestPostgres -count=1

resource-ctest:
	@test -n "$${RESOURCE_PROJECTION_TEST_DSN:-}" || (echo "RESOURCE_PROJECTION_TEST_DSN must point to a dedicated test database"; exit 1)
	@test -n "$${RESOURCE_KAFKA_TEST_BROKERS:-}" || (echo "RESOURCE_KAFKA_TEST_BROKERS must point to a dedicated test broker"; exit 1)
	go test -race ./internal/projection/resources -count=1

diagnostic-ctest:
	@test -n "$${DIAGNOSTIC_TEST_DSN:-}" || (echo "DIAGNOSTIC_TEST_DSN must point to a dedicated test database"; exit 1)
	go test -race ./internal/httpapi -run Diagnostic -count=1

activity-ctest:
	@test -n "$${STUDY_ACTIVITY_TEST_DSN:-}" || (echo "STUDY_ACTIVITY_TEST_DSN must point to a dedicated test database"; exit 1)
	go test -race -count=1 ./internal/studyactivity

evidence-ctest:
	@test -n "$${LEARNING_EVIDENCE_TEST_DSN:-}" || (echo "LEARNING_EVIDENCE_TEST_DSN must point to a dedicated test database"; exit 1)
	go test -race -count=1 ./internal/consumer

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; exit 1)

benchmark:
	go test ./internal/planner -run '^$$' -bench BenchmarkBuild -benchmem
	go test ./internal/learnergoals -run '^$$' -bench BenchmarkNormalizeLearnerGoal -benchmem
	go test ./internal/topiccatalog -run '^$$' -bench BenchmarkCatalogValidate16 -benchmem

tidy:
	go mod tidy

migrate-up:
	go run ./cmd/recommendations migrate up

$(GOLANGCI_LINT):
	GOBIN="$(LOCAL_BIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
