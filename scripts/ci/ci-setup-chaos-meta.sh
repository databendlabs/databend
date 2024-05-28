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

ls -lh ./target/"${BUILD_PROFILE}"
mkdir -p distro/amd64
cp ./target/"${BUILD_PROFILE}"/databend-meta ./distro/amd64/
cp ./target/"${BUILD_PROFILE}"/databend-metactl ./distro/amd64/
docker build -t databend-meta:meta-chaos --build-arg TARGETPLATFORM="amd64" -f ./docker/debian/meta.Dockerfile .
docker tag databend-meta:meta-chaos k3d-registry.localhost:5111/databend-meta:meta-chaos
docker push k3d-registry.localhost:5111/databend-meta:meta-chaos

echo "install chaos mesh on k3d"
curl -sSL https://mirrors.chaos-mesh.org/v2.6.3/install.sh | bash -s -- --k3s

kubectl get pods -A -o wide
kubectl get pvc -A

helm repo add databend https://charts.databend.rs
helm install test databend/databend-meta \
    --namespace databend \
    --create-namespace \
    --values scripts/ci/meta-chaos/meta-ha.yaml \
    --set image.repository=k3d-registry.localhost:5111/databend-meta \
    --set image.tag=meta-chaos \
    --wait || true

sleep 10
kubectl -n databend wait \
    --for=condition=ready pod \
    -l app.kubernetes.io/instance=meta-service \
    --timeout 120s || true

kubectl get pods -A -o wide
