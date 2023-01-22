fmt:
	cargo fmt

#docker

docker_up du:
	docker-compose up -d --build

docker_down dd:
	docker-compose down

build:
	docker build . -t soulgarden/logfowd2:0.0.3 --platform linux/amd64
	docker push soulgarden/logfowd2:0.0.3

#helm

create_namespace:
	kubectl create -f ./helm/namespace-logging.json

helm_install:
	helm install -n=logging logfowd helm/logfowd2 --wait

helm_upgrade:
	helm upgrade -n=logging logfowd helm/logfowd2 --wait

helm_delete:
	helm uninstall -n=logging logfowd
