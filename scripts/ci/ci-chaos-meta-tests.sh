#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "install k3d"
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG=v5.6.0 bash
echo "k3d create cluster"
k3d cluster create mycluster

echo "install chaos mesh on k3d"
curl -sSL https://mirrors.chaos-mesh.org/v2.0.2/install.sh | bash -s -- --k3s

echo "kubectl get pods"
kubectl get pods -o wide