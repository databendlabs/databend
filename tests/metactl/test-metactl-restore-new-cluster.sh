#!/bin/sh

set -o errexit

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

rm -fr .databend/

echo "start 3 meta node cluster"

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/databend-meta-node-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/databend-meta-node-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 28202

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/databend-meta-node-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 28302

curl -sL http://127.0.0.1:28101/v1/cluster/state

echo "stop 3 meta node cluster"
killall databend-meta
sleep 2

echo "export meta node data"

./target/debug/databend-metactl --export --raft-dir ./.databend/meta1 --db meta.db


rm -fr .databend/

echo "import old meta node data to new cluster"
./target/debug/databend-metactl --import --raft-dir ./.databend/new_meta1 --id=1 --db meta.db --initial-cluster 1=localhost:29103,0.0.0.0:19191 2=localhost:29203,0.0.0.0:29191 3=localhost:29303,0.0.0.0:39191
./target/debug/databend-metactl --import --raft-dir ./.databend/new_meta2 --id=2 --db meta.db --initial-cluster 1=localhost:29103,0.0.0.0:19191 2=localhost:29203,0.0.0.0:29191 3=localhost:29303,0.0.0.0:39191
./target/debug/databend-metactl --import --raft-dir ./.databend/new_meta3 --id=3 --db meta.db --initial-cluster 1=localhost:29103,0.0.0.0:19191 2=localhost:29203,0.0.0.0:29191 3=localhost:29303,0.0.0.0:39191

echo "start 3 new meta node cluster"
nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/new-databend-meta-node-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 19191

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/new-databend-meta-node-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 29191

nohup ./target/debug/databend-meta --config-file=./tests/metactl/config/new-databend-meta-node-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 39191

echo "check new cluster state"
curl -sL http://127.0.0.1:28101/v1/cluster/state | grep "\"voters\":\[{\"name\":\"1\",\"endpoint\":{\"addr\":\"localhost\",\"port\":29103},\"grpc_api_addr\":\"0.0.0.0:19191\"},{\"name\":\"2\",\"endpoint\":{\"addr\":\"localhost\",\"port\":29203},\"grpc_api_addr\":\"0.0.0.0:29191\"},{\"name\":\"3\",\"endpoint\":{\"addr\":\"localhost\",\"port\":29303},\"grpc_api_addr\":\"0.0.0.0:39191\"}\]"

killall databend-meta