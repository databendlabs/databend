#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../shell_env.sh

WAREHOUSE="${PAIMON_WAREHOUSE_PATH:-${TESTS_DATA_DIR}/paimon_warehouse}"
export PAIMON_WAREHOUSE="${WAREHOUSE}"
mkdir -p "${WAREHOUSE}"

# Spark 3.5 / Paimon need Java 17+. Prefer a known JDK when JAVA_HOME is unset.
if [[ -z "${JAVA_HOME:-}" ]]; then
	for candidate in \
		"${HOME}/Library/Java/JavaVirtualMachines/corretto-17.0.14/Contents/Home" \
		"${HOME}/Library/Java/JavaVirtualMachines/corretto-21.0.6/Contents/Home" \
		"/usr/lib/jvm/java-17" \
		"/usr/lib/jvm/java-21"; do
		if [[ -x "${candidate}/bin/java" ]]; then
			export JAVA_HOME="${candidate}"
			break
		fi
	done
	if [[ -z "${JAVA_HOME:-}" ]] && command -v /usr/libexec/java_home >/dev/null 2>&1; then
		export JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || /usr/libexec/java_home -v 21 2>/dev/null || true)"
	fi
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
	export PATH="${JAVA_HOME}/bin:${PATH}"
fi

uv run --script "${CURDIR}/prepare_paimon_fs_data.py"
