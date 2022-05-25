#!/bin/sh

usage()
{
    echo "test query being compatible with old metasrv"
    echo "Expect current release binaries are in ./current/."
    echo "Expect     old release binaries are in ./old/."
    echo "Usage: $0"
}

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

echo "SCRIPT_PATH: $SCRIPT_PATH"

# go to work tree root
cd "$SCRIPT_PATH/../../"

pwd

mkdir -p ./target/debug/
chmod +x ./current/*
chmod +x ./old/*

cp ./old/databend-query    ./target/debug/
cp ./current/databend-meta ./target/debug/

./target/debug/databend-meta --version
./target/debug/databend-query --version

# Not all cases in the latest repo will pass
echo "Run some specific SQL tests."

echo "Starting standalone DatabendQuery(debug)"

killall databend-query
killall databend-meta
sleep 1

for bin in databend-query databend-meta; do
	if test -n "$(pgrep $bin)"; then
		echo "The $bin is not killed. force killing."
		killall -9 $bin
	fi
done

echo 'Start databend-meta...'
nohup target/debug/databend-meta --single --log-level=DEBUG &
echo "Waiting on databend-meta 10 seconds..."
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191

echo 'Start databend-query...'
nohup target/debug/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml &

echo "Waiting on databend-query 10 seconds..."
python3 scripts/ci/wait_tcp.py --timeout 5 --port 3307

cd tests

echo "Starting metasrv related test: 05_ddl"
./databend-test --mode 'standalone' --run-dir 0_stateless -- 05_
