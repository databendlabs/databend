#!/bin/bash

# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-./target}
DATABEND_USER=${DATABEND_USER:-root}
DATABEND_PASSWORD=${DATABEND_PASSWORD:-}
DATABEND_HOST=${DATABEND_HOST:-localhost}

TEST_HANDLER=$1

cargo build --bin bendsql

case $TEST_HANDLER in
"flight")
	echo "==> Testing Flight SQL handler"
	export BENDSQL_DSN="databend+flight://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8900/?sslmode=disable"
	;;
"http")
	echo "==> Testing REST API handler"
	export BENDSQL_DSN="databend+http://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8000/?sslmode=disable&presign=on"
	;;
*)
	echo "Usage: $0 [flight|http]"
	exit 1
	;;
esac

export BENDSQL="${CARGO_TARGET_DIR}/debug/bendsql"

for tf in cli/tests/*.{sql,sh}; do
	[[ -e "$tf" ]] || continue
	echo "    Running test -- ${tf}"
	if [[ $tf == *.sh ]]; then
		suite=$(basename "${tf}" | sed -e 's#.sh##')
		bash "${tf}" >"cli/tests/${suite}.output" 2>&1 || true
	elif [[ $tf == *.sql ]]; then
		suite=$(basename "${tf}" | sed -e 's#.sql##')
		"${BENDSQL}" --output tsv <"${tf}" >"cli/tests/${suite}.output" 2>&1 || true
	fi
	diff "cli/tests/${suite}.output" "cli/tests/${suite}.result"
done
rm -f cli/tests/*.output

for tf in cli/tests/"$TEST_HANDLER"/*.{sql,sh}; do
	[[ -e "$tf" ]] || continue
	echo "    Running test -- ${tf}"
	if [[ $tf == *.sh ]]; then
		suite=$(basename "${tf}" | sed -e 's#.sh##')
		bash "${tf}" >"cli/tests/${TEST_HANDLER}/${suite}.output" 2>&1 || true
	elif [[ $tf == *.sql ]]; then
		suite=$(basename "${tf}" | sed -e 's#.sql##')
		"${BENDSQL}" --output tsv <"${tf}" >"cli/tests/${TEST_HANDLER}/${suite}.output" 2>&1 || true
	fi
	diff "cli/tests/${TEST_HANDLER}/${suite}.output" "cli/tests/${TEST_HANDLER}/${suite}.result"
done
rm -f cli/tests/"$TEST_HANDLER"/*.output

echo "--> Tests $1 passed"
echo
