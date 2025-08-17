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

#docker

docker_up du:
	docker-compose up -d --build

docker_down dd:
	docker-compose down

build:
	docker build . -t soulgarden/logfowd2:0.0.7 --platform linux/amd64
	docker push soulgarden/logfowd2:0.0.7

#helm

create_namespace:
	kubectl create -f ./helm/namespace-logging.json

helm_install:
	helm install -n=logging logfowd helm/logfowd2 --wait

helm_upgrade:
	helm upgrade -n=logging logfowd helm/logfowd2 --wait

helm_delete:
	helm uninstall -n=logging logfowd
