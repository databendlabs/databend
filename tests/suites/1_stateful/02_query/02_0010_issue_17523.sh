#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# Skip in standalone mode
port_check=$(sudo lsof -i :9093)
if [ -z "$port_check" ]; then
    echo -e "1\n1\n1\n1\ntest_text1\ntest_text2\ntest_text3\ntest_text4\n2\ntest_text1\ntest_text2\ntest_text3\ntest_text4\n3"
    exit 0
fi

echo "drop table if exists table_test_02_0010;" | $BENDSQL_CLIENT_CONNECT

echo "create table table_test_02_0010(TEXT String);" | $BENDSQL_CLIENT_CONNECT


table_inserts=(
  "INSERT INTO table_test_02_0010 VALUES('test_text1');"
  "INSERT INTO table_test_02_0010 VALUES('test_text2');"
  "INSERT INTO table_test_02_0010 VALUES('test_text3');"
  "INSERT INTO table_test_02_0010 VALUES('test_text4');"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $BENDSQL_CLIENT_CONNECT
done


echo "select * from table_test_02_0010 order by text;" | $BENDSQL_CLIENT_CONNECT

# kill a node
sudo lsof -i :9093 -t | xargs -r sudo kill -9
# wait for the node to be killed
sleep 5

echo "select count(*) from system.clusters;" | $BENDSQL_CLIENT_CONNECT

echo "select * from table_test_02_0010 order by text;" | $BENDSQL_CLIENT_CONNECT

# restart the node
ROOTDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../../../../ && pwd)"
env "RUST_BACKTRACE=1" nohup "$ROOTDIR"/target/${BUILD_PROFILE}/databend-query -c "$ROOTDIR"/scripts/ci/deploy/config/databend-query-node-3.toml --internal-enable-sandbox-tenant > "$ROOTDIR"/.databend/query-3.out 2>&1 &
python3 "$ROOTDIR"/scripts/ci/wait_tcp.py --timeout 30 --port 9093 > /dev/null 2>&1

echo "select count(*) from system.clusters;" | $BENDSQL_CLIENT_CONNECT
