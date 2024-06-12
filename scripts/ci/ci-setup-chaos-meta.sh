#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -ex

BUILD_PROFILE=${BUILD_PROFILE:-debug}

curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.6.0 bash
k3d registry create registry.localhost --port 0.0.0.0:5111 -i registry:latest
k3d cluster create --config ./scripts/ci/meta-chaos/k3d.yaml meta-chaos

echo "127.0.0.1 k3d-registry.localhost" | sudo tee -a /etc/hosts

if kubectl version --client; then
	echo "kubectl client already installed"
else
	echo "install kubectl client"
	curl -LO "https://dl.k8s.io/release/v1.29.5/bin/linux/amd64/kubectl"
	chmod +x kubectl
	sudo mv kubectl /usr/local/bin/
fi

if helm version; then
	echo "helm already installed"
else
	echo "install helm"
	curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
	chmod 700 get_helm.sh
	./get_helm.sh
fi

echo "make databend-meta image"
ls -lh ./target/"${BUILD_PROFILE}"
mkdir -p temp/distro/amd64
cp ./target/"${BUILD_PROFILE}"/databend-meta ./temp/distro/amd64
cp ./target/"${BUILD_PROFILE}"/databend-metactl ./temp/distro/amd64
cp tests/metaverifier/cat-logs.sh ./temp/distro/amd64
docker build -t databend-meta:meta-chaos --build-arg TARGETPLATFORM="amd64" -f ./docker/chaos-meta/meta.Dockerfile temp
docker tag databend-meta:meta-chaos k3d-registry.localhost:5111/databend-meta:meta-chaos
docker push k3d-registry.localhost:5111/databend-meta:meta-chaos

echo "make databend-metaverifier image"
rm -rf temp/distro/amd64/*
cp ./target/"${BUILD_PROFILE}"/databend-metaverifier ./temp/distro/amd64
cp tests/metaverifier/start-verifier.sh ./temp/distro/amd64
cp tests/metaverifier/cat-logs.sh ./temp/distro/amd64
docker build -t databend-metaverifier:meta-chaos --build-arg TARGETPLATFORM="amd64" -f ./docker/chaos-meta/verifier.Dockerfile temp
docker tag databend-metaverifier:meta-chaos k3d-registry.localhost:5111/databend-metaverifier:meta-chaos
docker push k3d-registry.localhost:5111/databend-metaverifier:meta-chaos

echo "install chaos mesh on k3d"
curl -sSL https://mirrors.chaos-mesh.org/v2.6.3/install.sh | bash -s -- --k3s

kubectl get pods -A -o wide
kubectl get pvc -A

echo "kubectl delete databend-meta pvc"
kubectl delete pvc --namespace databend data-test-databend-meta-0 data-test-databend-meta-1 data-test-databend-meta-2 --ignore-not-found

helm repo add databend https://charts.databend.rs
helm install test databend/databend-meta \
	--namespace databend \
	--create-namespace \
	--values scripts/ci/meta-chaos/meta-ha.yaml \
	--set image.repository=k3d-registry.localhost:5111/databend-meta \
	--set image.tag=meta-chaos \
	--wait || true

sleep 10
echo "check if databend-meta nodes is ready"
kubectl -n databend wait \
	--for=condition=ready pod \
	-l app.kubernetes.io/name=databend-meta \
	--timeout 120s || true

kubectl get pods -A -o wide

kubectl -n databend exec test-databend-meta-0 -- /databend-metactl --status

echo "create verifier pod.."
kubectl apply -f scripts/ci/meta-chaos/verifier.yaml

echo "check if databend-metaverifier node is ready"
kubectl -n databend wait \
	--for=condition=ready pod \
	-l app.kubernetes.io/name=databend-metaverifier \
	--timeout 120s || true

echo "logs databend-metaverifier.."
kubectl logs databend-metaverifier --namespace databend

kubectl get pods -n databend -o wide
