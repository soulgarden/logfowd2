VERSION := $(shell cat VERSION)

.PHONY: fmt lint test check build_dev build_release helm_validate dev_check ci \
        docker_up docker_down build docker-build docker-build-local \
        create_namespace helm_install helm_upgrade helm_delete \
        get-version increment-version release-patch release-minor release-major

# Development Commands
fmt:
	cargo fmt --all

lint:
	cargo clippy --fix --allow-dirty

test:
	cargo test -- --test-threads=1

check:
	cargo check

build_dev:
	cargo build

build_release:
	cargo build --release

# Validate Helm charts
helm_validate:
	helm lint helm/logfowd2
	helm template logfowd helm/logfowd2 --validate --dry-run

# Run all development checks (format, lint, test, build, helm validation)
dev_check: fmt lint test check build_dev helm_validate
	@echo "All development checks passed!"

# Run CI pipeline (check, test, build)
ci: check test build_release
	@echo "CI pipeline completed successfully!"

# Versioning Commands
get-version:
	@echo $(VERSION)

increment-version:
	@current_version=$$(cat VERSION); \
	major=$$(echo $$current_version | cut -d. -f1); \
	minor=$$(echo $$current_version | cut -d. -f2); \
	patch=$$(echo $$current_version | cut -d. -f3); \
	minor=$$((minor + 1)); \
	new_version="$$major.$$minor.$$patch"; \
	echo "Incrementing version from $$current_version to $$new_version"; \
	echo $$new_version > VERSION

# Docker Commands
docker_up du:
	docker-compose up -d --build

docker_down dd:
	docker-compose down

# Build and push Docker image with version from VERSION file
docker-build db:
	@NEW_VERSION=$$(cat VERSION); \
	echo "Building with version: $$NEW_VERSION"; \
	docker build . -t soulgarden/logfowd2:$$NEW_VERSION -t soulgarden/logfowd2:latest --platform linux/amd64; \
	docker push soulgarden/logfowd2:$$NEW_VERSION; \
	docker push soulgarden/logfowd2:latest

# Helm Commands
create_namespace:
	kubectl create -f ./helm/namespace-logging.json

helm_install:
	helm install -n=logging logfowd helm/logfowd2 --wait \
		--set image.tag=$(VERSION)

helm_upgrade:
	helm upgrade -n=logging logfowd helm/logfowd2 --wait \
		--set image.tag=$(VERSION)

helm_delete:
	helm uninstall -n=logging logfowd
