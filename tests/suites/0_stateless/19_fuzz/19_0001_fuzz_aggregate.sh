#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


rows=1000


echo """
create or replace table agg_fuzz(a int, b string, c bool, d int, e Decimal(15, 2), f Decimal(39,2));
create or replace table agg_fuzz_r like agg_fuzz Engine = Random max_string_len = 3 max_array_len = 2;
""" | $BENDSQL_CLIENT_OUTPUT_NULL


echo """
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
""" | $BENDSQL_CLIENT_OUTPUT_NULL

for m in `seq 1 3 10`; do
	echo """create or replace table agg_fuzz_result1 as select a, sum(d) d , sum(e) e, sum(f) f from (
select a, c, sum(d) d , sum(e) e, sum(f) f  from (
select a % ${m} a, b, c, sum(d) d , sum(e) e, sum(f) f  from agg_fuzz group by all
) group by all
) group by all;
""" | $BENDSQL_CLIENT_OUTPUT_NULL

	echo """create or replace table agg_fuzz_result2 as select a, sum(d) d , sum(e) e, sum(f) f from (
select a, b, sum(d) d , sum(e) e, sum(f) f  from (
select a % ${m} a, b, c, sum(d) d , sum(e) e, sum(f) f  from agg_fuzz group by all
) group by all
) group by all;
""" | $BENDSQL_CLIENT_OUTPUT_NULL

	echo "RESULT--${m}"
	## judge the result are same
	echo """
	SELECT * FROM agg_fuzz_result1
	EXCEPT
	SELECT * FROM agg_fuzz_result2
	UNION ALL
	SELECT * FROM agg_fuzz_result2
	EXCEPT
	SELECT * FROM agg_fuzz_result1;
	""" | $BENDSQL_CLIENT_CONNECT
done
