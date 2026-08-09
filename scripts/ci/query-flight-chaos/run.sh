#!/usr/bin/env bash
# Copyright 2020-2026 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -euo pipefail

readonly KIND_VERSION="v0.29.0"
readonly KUBERNETES_VERSION="v1.33.1"
readonly HELM_VERSION="v3.18.6"
readonly CHAOS_MESH_VERSION="2.8.3"
readonly NAMESPACE="databend-chaos"
readonly IMAGE="databend-query-flight-chaos:ci"
readonly BUILD_PROFILE="${BUILD_PROFILE:-debug}"
readonly CLUSTER_NAME="databend-query-chaos-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly LOG_DIR="${REPO_DIR}/.databend/query-flight-chaos"

tools_dir="$(mktemp -d)"
build_context="$(mktemp -d)"
port_forward_pid=""

collect_diagnostics() {
	mkdir -p "${LOG_DIR}"
	kubectl get pods -A -o wide >"${LOG_DIR}/pods.txt" 2>&1 || true
	kubectl get networkchaos -A -o yaml >"${LOG_DIR}/network-chaos.yaml" 2>&1 || true
	kubectl -n "${NAMESPACE}" describe pods >"${LOG_DIR}/describe-pods.txt" 2>&1 || true
	for pod in databend-query-0 databend-query-1 databend-query-2; do
		kubectl -n "${NAMESPACE}" logs "${pod}" -c query >"${LOG_DIR}/${pod}.log" 2>&1 || true
	done
	kubectl -n "${NAMESPACE}" logs deployment/databend-meta >"${LOG_DIR}/databend-meta.log" 2>&1 || true
}

cleanup() {
	status=$?
	trap - EXIT
	set +e
	collect_diagnostics
	if [[ -n "${port_forward_pid}" ]]; then
		kill "${port_forward_pid}" 2>/dev/null || true
		wait "${port_forward_pid}" 2>/dev/null || true
	fi
	kind delete cluster --name "${CLUSTER_NAME}"
	docker image rm "${IMAGE}" >/dev/null 2>&1 || true
	rm -rf -- "${tools_dir}" "${build_context}"
	exit "${status}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

case "$(uname -m)" in
x86_64)
	readonly binary_arch="amd64"
	;;
aarch64 | arm64)
	readonly binary_arch="arm64"
	;;
*)
	echo "Unsupported architecture: $(uname -m)" >&2
	exit 1
	;;
esac

curl --fail --location --retry 3 \
	-o "${tools_dir}/kind" \
	"https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${binary_arch}"
curl --fail --location --retry 3 \
	-o "${tools_dir}/kubectl" \
	"https://dl.k8s.io/release/${KUBERNETES_VERSION}/bin/linux/${binary_arch}/kubectl"
curl --fail --location --retry 3 \
	-o "${tools_dir}/helm.tar.gz" \
	"https://get.helm.sh/helm-${HELM_VERSION}-linux-${binary_arch}.tar.gz"
tar -xzf "${tools_dir}/helm.tar.gz" -C "${tools_dir}"
mv "${tools_dir}/linux-${binary_arch}/helm" "${tools_dir}/helm"
chmod +x "${tools_dir}/kind" "${tools_dir}/kubectl" "${tools_dir}/helm"
export PATH="${tools_dir}:${PATH}"

kind create cluster \
	--name "${CLUSTER_NAME}" \
	--image "kindest/node:${KUBERNETES_VERSION}" \
	--wait 120s

helm repo add chaos-mesh https://charts.chaos-mesh.org
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
	--namespace chaos-mesh \
	--create-namespace \
	--version "${CHAOS_MESH_VERSION}" \
	--set chaosDaemon.runtime=containerd \
	--set chaosDaemon.socketPath=/run/containerd/containerd.sock \
	--set dashboard.create=false \
	--wait \
	--timeout 5m

install -m 0755 "${REPO_DIR}/target/${BUILD_PROFILE}/databend-query" "${build_context}/databend-query"
install -m 0755 "${REPO_DIR}/target/${BUILD_PROFILE}/databend-meta" "${build_context}/databend-meta"
docker build --tag "${IMAGE}" --file "${SCRIPT_DIR}/Dockerfile" "${build_context}"
kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"

kubectl apply -f "${SCRIPT_DIR}/databend.yaml"
kubectl -n "${NAMESPACE}" rollout status deployment/databend-meta --timeout=120s
kubectl -n "${NAMESPACE}" rollout status statefulset/databend-query --timeout=180s
kubectl -n "${NAMESPACE}" label pod databend-query-0 chaos-role=coordinator --overwrite
kubectl -n "${NAMESPACE}" label pod databend-query-1 databend-query-2 chaos-role=worker --overwrite

mkdir -p "${LOG_DIR}"
kubectl -n "${NAMESPACE}" port-forward pod/databend-query-0 3307:3307 \
	>"${LOG_DIR}/port-forward.log" 2>&1 &
port_forward_pid=$!

python "${REPO_DIR}/tests/query-flight-chaos/test_reconnect.py" \
	--namespace "${NAMESPACE}" \
	--network-chaos "${SCRIPT_DIR}/network-chaos.yaml"
