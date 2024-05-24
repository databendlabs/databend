#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "install k3d"
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.6.0 bash
echo "k3d create cluster"
k3d cluster create mycluster

echo "install kubectl"
curl -LO https://dl.k8s.io/release/v1.28.0/bin/linux/amd64/kubectl
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

echo "install chaos mesh on k3d"
curl -sSL https://mirrors.chaos-mesh.org/v2.6.3/install.sh | bash -s -- --k3s

echo "kubectl get pods"
kubectl get pods -o wide

echo "install helm"
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
helm version

echo "heml start meta cluster"
helm repo add databend https://charts.databend.rs
helm install my-release databend/databend-meta --namespace databend --create-namespace --values scripts/ci/meta-chaos/meta-ha.yaml

echo "get meta pods"
kubectl get pods -o wide --namespace databend