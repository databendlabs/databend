#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


rows=100000


echo """
create or replace table agg_fuzz(a int, b string, c bool, d variant, e int, f Decimal(15, 2), g Decimal(39,2));
create or replace table agg_fuzz_r like agg_fuzz Engine = Random max_string_len = 3 max_array_len = 2;
""" | $BENDSQL_CLIENT_OUTPUT_NULL


echo """
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
insert into agg_fuzz select * from agg_fuzz_r limit ${rows};
""" | $BENDSQL_CLIENT_OUTPUT_NULL

for m in `seq 1 3 20`; do
	echo """create or replace table agg_fuzz_result1 as select a, sum(e) e, sum(f) f, sum(g) g from (
	select a, d, sum(e) e, sum(f) f, sum(g) g from (
select a, c, d, sum(e) e, sum(f) f, sum(g) g  from (
select a % ${m} a, b, c, d, sum(e) e, sum(f) f, sum(g) g  from agg_fuzz where b >= 'R' group by all
) group by all
) group by all
) group by all;
""" | $BENDSQL_CLIENT_OUTPUT_NULL

	echo """create or replace table agg_fuzz_result2 as select a, sum(e) e, sum(f) f, sum(g) g from (
	select a, d, sum(e) e, sum(f) f, sum(g) g from (
select a, c, d, sum(e) e, sum(f) f, sum(g) g  from (
select a % ${m} a, b, c, d, sum(e) e, sum(f) f, sum(g) g  from agg_fuzz where b >= 'R' group by all
) group by all
) group by all
) group by all;
""" | $BENDSQL_CLIENT_OUTPUT_NULL

	echo """
	select count() + 1 from (
		SELECT * FROM agg_fuzz_result1
		EXCEPT
		SELECT * FROM agg_fuzz_result2
		UNION ALL
		SELECT * FROM agg_fuzz_result2
		EXCEPT
		SELECT * FROM agg_fuzz_result1
	);
	""" | $BENDSQL_CLIENT_CONNECT

	echo """
	select count() + 1 from (
		(SELECT * FROM agg_fuzz_result1
		EXCEPT
		SELECT * FROM agg_fuzz_result2)
		UNION ALL
		(SELECT * FROM agg_fuzz_result2
		EXCEPT
		SELECT * FROM agg_fuzz_result1)
	);
	""" | $BENDSQL_CLIENT_CONNECT
done
