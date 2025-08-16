GOLANGCI_LINT_VERSION = v2.4.0

GOFILES_NOVENDOR = $(shell find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./.git/*")

# Version environment variable to use in the build process
GITVERSION = $(shell gitversion | jq .SemVer)
GITVERSION_NOT_INSTALLED = "gitversion is not installed: https://github.com/GitTools/GitVersion"

BUF_VERSION = v1.55.1

test:
	go test -v -timeout 5m -cover ./...

bench:
	cd tests && go test -bench=. -benchmem -benchtime=4s . -timeout 30m

update-deps:
	go get -v -u ./...
	go mod tidy

vet:
	go vet ./...

prepare-proto-tools:
	@echo "Installing buf $(BUF_VERSION)..."
	$(call check_command_exists,buf) || go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

	@echo "Installing protoc-gen-go..."
	$(call check_command_exists,protoc-gen-go) || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

	@echo "Installing protoc-gen-go-grpc..."
	$(call check_command_exists,protoc-gen-go-grpc) || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

	@echo "Installing protoc-gen-openapi..."
	$(call check_command_exists,protoc-gen-openapi) || go install github.com/google/gnostic/cmd/protoc-gen-openapi@latest

proto-update:
	buf dep update

proto-lint:
	buf lint

proto-breaking:
	buf breaking --against '.git#branch=main'

proto-generate:
	buf generate

proto-format: ## Format proto files
	buf format -w

proto: proto-update proto-lint proto-generate proto-format


prepare-toolchain: prepare-proto-tools
	$(call check_command_exists,docker) || (echo "Docker is missing, install it before starting to code." && exit 1)

	$(call check_command_exists,git) || (echo "git is not present on the system, install it before starting to code." && exit 1)

	$(call check_command_exists,go) || (echo "golang is not present on the system, download and install it at https://go.dev/dl" && exit 1)

	$(call check_command_exists,gitversion) || (echo "${GITVERSION_NOT_INSTALLED}" && exit 1)

	@echo "Installing gci...\n"
	$(call check_command_exists,gci) || go install github.com/daixiang0/gci@latest

	@echo "Installing gofumpt...\n"
	$(call check_command_exists,gofumpt) || go install mvdan.cc/gofumpt@latest

	@echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)...\n"
	$(call check_command_exists,golangci-lint) || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$$(go env GOPATH)/bin" $(GOLANGCI_LINT_VERSION)

	@echo "Installing staticcheck...\n"
	$(call check_command_exists,staticcheck) || go install honnef.co/go/tools/cmd/staticcheck@latest

	@echo "Checking if pre-commit is installed..."
	pre-commit --version || (echo "pre-commit is not installed, install it with 'pip install pre-commit'" && exit 1)

	@echo "Initializing pre-commit..."
	pre-commit validate-config || pre-commit install && pre-commit install-hooks


lint: prepare-toolchain vet
	@echo "Running gci..."
	@for file in ${GOFILES_NOVENDOR}; do \
		gci write -s standard -s default -s blank -s dot -s "prefix(github.com/hyp3rd/go-worker)" -s localmodule --skip-vendor --skip-generated $$file; \
	done

	@echo "\nRunning gofumpt..."
	gofumpt -l -w ${GOFILES_NOVENDOR}

	@echo "\nRunning staticcheck..."
	staticcheck ./...

	@echo "\nRunning golangci-lint $(GOLANGCI_LINT_VERSION)..."
	golangci-lint run -v --fix ./...

# check_command_exists is a helper function that checks if a command exists.
define check_command_exists
@which $(1) > /dev/null 2>&1 || (echo "$(1) command not found" && exit 1)
endef

ifeq ($(call check_command_exists,$(1)),false)
  $(error "$(1) command not found")
endif

# help prints a list of available targets and their descriptions.
help:
	@echo "Available targets:"
	@echo
	@echo "Development commands:"
	@echo "  prepare-toolchain\t\tInstall and configure all required development tools"
	@echo
	@echo "Testing commands:"
	@echo "  test\t\t\t\tRun all tests in the project"
	@echo "  bench\t\t\t\tRun benchmarks with 4 seconds of execution time"
	@echo
	@echo "Code quality commands:"
	@echo "  lint\t\t\t\tRun all linters (gci, gofumpt, staticcheck, golangci-lint)"
	@echo "  update-deps\t\t\tUpdate all dependencies and tidy go.mod"
	@echo
	@echo
	@echo "For more information, see the project README."

.PHONY: prepare-toolchain test bench vet update-deps lint help
