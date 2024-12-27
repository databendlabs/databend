#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


rows=1000


echo """
create or replace table union_fuzz(a int, b string, c bool, d variant, e int64, f Decimal(15, 2), g Decimal(39,2), h Array(String), i Array(Decimal(15, 2)));
create or replace table union_fuzz2(a int, b string, c bool, d variant, e int64, f Decimal(15, 2), g Decimal(39,2), h Array(String), i Array(Decimal(15, 2)));
create or replace table union_fuzz_r like union_fuzz Engine = Random max_string_len = 5 max_array_len = 2;
""" | $BENDSQL_CLIENT_OUTPUT_NULL


echo """
insert into union_fuzz select * from union_fuzz_r limit ${rows};
insert into union_fuzz select * from union_fuzz_r limit ${rows};
insert into union_fuzz2 select * from union_fuzz_r limit ${rows};
insert into union_fuzz2 select * from union_fuzz_r limit ${rows};
""" | $BENDSQL_CLIENT_OUTPUT_NULL

fields=(a b c d e f g)
length=${#fields[@]}

for ((i=0; i<$length; i++)); do
    for ((j=i+1; j<$length; j++)); do
        x=${fields[$i]}
        y=${fields[$j]}

	# x,y
	echo """create or replace table union_fuzz_result1 as
	select $x, $y from union_fuzz union all select $x, $y from union_fuzz2;
""" | $BENDSQL_CLIENT_OUTPUT_NULL

	echo """create or replace table union_fuzz_result2 as
	select $y, $x from (select $x, $y from union_fuzz2 union all select $x, $y from union_fuzz);
""" | $BENDSQL_CLIENT_OUTPUT_NULL

echo """
	select count() + 1 from (
		(SELECT $x, $y FROM union_fuzz_result1
		EXCEPT
		SELECT $x, $y FROM union_fuzz_result2)
		UNION ALL
		(SELECT $x, $y FROM union_fuzz_result2
		EXCEPT
		SELECT $x, $y FROM union_fuzz_result1)
	);
	""" | $BENDSQL_CLIENT_CONNECT

    done
done
