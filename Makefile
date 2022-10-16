fmt:
	cargo fmt

build:
	docker build . -t soulgarden/logfowd2:0.0.1 --platform linux/amd64
	docker push soulgarden/logfowd2:0.0.1

create_namespace:
	kubectl create -f ./helm/namespace-logging.json

helm_install:
	helm install -n=logging logfowd helm/logfowd2 --wait

helm_upgrade:
	helm upgrade -n=logging logfowd helm/logfowd2 --wait

helm_delete:
	helm uninstall logfowd2 -n=logging
