#!/bin/sh

set -e

CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-./target}
DATABEND_USER=${DATABEND_USER:-root}
DATABEND_PASSWORD=${DATABEND_PASSWORD:-}
DATABEND_HOST=${DATABEND_HOST:-localhost}

case $1 in
"flight")
	echo
	echo "==> Testing Flight SQL handler"
	export BENDSQL_DSN="databend+flight://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8900/?sslmode=disable"
	;;
"http")
	echo
	echo "==> Testing REST API handler"
	export BENDSQL_DSN="databend+http://${DATABEND_USER}:${DATABEND_PASSWORD}@${DATABEND_HOST}:8000/?sslmode=disable"
	;;
*)
	echo "Usage: $0 [flight|http]"
	exit 1
	;;
esac

cargo build --bin bendsql

for tf in cli/tests/*.sql; do
	echo "    Running test -- ${tf}"
	suite=$(basename "${tf}" | sed -e 's#.sql##')
	"${CARGO_TARGET_DIR}/debug/bendsql" <"${tf}" >"cli/tests/${suite}.output" 2>&1
	diff "cli/tests/${suite}.output" "cli/tests/${suite}.result"
done

rm cli/tests/*.output

echo "--> Tests $1 passed"
