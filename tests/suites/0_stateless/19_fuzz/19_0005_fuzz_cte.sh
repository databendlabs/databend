#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


times=4

echo "" > /tmp/fuzz_a.txt
echo "" > /tmp/fuzz_b.txt

read -r -d '' FUZZ_QUERY <<'SQL'
with t0(sum_sid) as (
  select sum(number) over(partition by number order by number)
  from numbers(3)
)
select n, if(n = 1, sum_sid + 1, 0)
from t0, (select 1 n union all select 2)
order by 1, 2;
SQL

for i in `seq 1 ${times}`;do
	echo "$FUZZ_QUERY" | $BENDSQL_CLIENT_CONNECT >> /tmp/fuzz_a.txt
done


for i in `seq 1 ${times}`;do
	echo "$FUZZ_QUERY" | $BENDSQL_CLIENT_CONNECT >> /tmp/fuzz_b.txt
done

diff /tmp/fuzz_a.txt /tmp/fuzz_b.txt && echo "OK"
