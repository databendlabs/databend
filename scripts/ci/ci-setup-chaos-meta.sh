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
kubectl delete pvc --namespace databend  data-my-release-databend-meta-0 data-my-release-databend-meta-1 data-my-release-databend-meta-2 2>/dev/null

echo "install helm"
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
helm version

echo "heml start meta cluster"
helm repo add databend https://charts.databend.rs
helm install my-release databend/databend-meta --namespace databend --create-namespace --values scripts/ci/meta-chaos/meta-ha.yaml --wait

echo "check meta Running pods"
MAX_WAIT_TIME=15
countdown=$MAX_WAIT_TIME

while [ $countdown -gt 0 ]; do
    kubectl logs --namespace databend my-release-databend-meta-0 | tail -n 50
    output=$(kubectl get pods -o wide --namespace databend | grep Running)

    LINE_COUNT=$(echo "$output" | wc -l)

    if [ $LINE_COUNT -eq 3 ]; then
        echo "Success: The output has exactly 3 Running pods."
        exit 0
    fi

    echo "try to check meta Running pods again.."
    ((countdown--))

    sleep 1
done

echo "Error: Timeout after $MAX_WAIT_TIME seconds."
exit 1
