#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "install k3s.."
curl -sfL https://rancher-mirror.rancher.cn/k3s/k3s-install.sh | INSTALL_K3S_MIRROR=cn sh -

echo "install chaos mesh"
curl -sSL https://mirrors.chaos-mesh.org/v2.0.2/install.sh | bash -s -- --k3s

echo "kubectl get pods"
kubectl get pods -o wide