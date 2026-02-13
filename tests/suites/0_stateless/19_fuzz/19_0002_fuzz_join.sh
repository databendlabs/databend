#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_bendsql() {
  cat <<SQL | $BENDSQL_CLIENT_CONNECT
$1
SQL
}

run_bendsql_null() {
  cat <<SQL | $BENDSQL_CLIENT_OUTPUT_NULL
$1
SQL
}

## fuzz join
## ideas
# The logical relationships of JOIN:

# A. The relationship between LEFT JOIN and RIGHT JOIN:
#    - 1. All rows in LEFT JOIN should appear in FULL OUTER JOIN.
#    - 2. All rows in RIGHT JOIN should also appear in FULL OUTER JOIN.
#    - 3. FULL OUTER JOIN is the union of LEFT JOIN and RIGHT JOIN.

# B. The complementarity of LEFT JOIN and RIGHT JOIN:
#    - 1. If certain rows in the result of LEFT JOIN have NULLs in the right table, these rows should have no corresponding entries in RIGHT JOIN.
#    - 2. Similarly, if certain rows in the result of RIGHT JOIN have NULLs in the left table, these rows should have no corresponding entries in LEFT JOIN.

# C. Verifying the decomposability of FULL OUTER JOIN:
#    - 1. FULL OUTER JOIN = LEFT JOIN + RIGHT JOIN - overlapping parts.

rows=1000

# Create tables for join fuzz testing
run_bendsql_null "
create or replace table join_fuzz1(a int, b string, c bool, d variant, e int64, f Decimal(15, 2), g Decimal(39,2), h Array(String), i Array(Decimal(15, 2)));
create or replace table join_fuzz2(a int, b string, c bool, d variant, e int64, f Decimal(15, 2), g Decimal(39,2), h Array(String), i Array(Decimal(15, 2)));
create or replace table join_fuzz_r like join_fuzz1 Engine = Random max_string_len = 5 max_array_len = 2;
"

# Insert data into the tables
run_bendsql_null "
insert into join_fuzz1 select * from join_fuzz_r limit ${rows};
insert into join_fuzz2 select * from join_fuzz_r limit ${rows};
"

fields=(a b e f)
length=${#fields[@]}

# Perform join operations and compare results

for ((i=0; i<$length; i++)); do
    for ((j=i+1; j<$length; j++)); do
        x=${fields[$i]}
        y=${fields[$j]}

	run_bendsql_null "
create or replace table join_fuzz_result1 as
        select join_fuzz1.$x, join_fuzz1.$y from join_fuzz1 left join join_fuzz2 on join_fuzz1.$x = join_fuzz2.$x and join_fuzz1.$y > join_fuzz2.$y;
"

        run_bendsql_null "
create or replace table join_fuzz_result2 as
        select join_fuzz1.$x, join_fuzz1.$y from join_fuzz2 right join join_fuzz1 on join_fuzz2.$x = join_fuzz1.$x and join_fuzz1.$y > join_fuzz2.$y;
"

	## A.1 and A.2
        run_bendsql "
        select count() + 1 from (
            (SELECT join_fuzz_result1.$x, join_fuzz_result1.$y FROM join_fuzz_result1
            EXCEPT
            SELECT join_fuzz_result2.$x, join_fuzz_result2.$y FROM join_fuzz_result2)
            UNION ALL
            (SELECT join_fuzz_result2.$x, join_fuzz_result2.$y FROM join_fuzz_result2
            EXCEPT
            SELECT join_fuzz_result1.$x, join_fuzz_result1.$y FROM join_fuzz_result1)
        );
        "

	run_bendsql_null "
create or replace table join_fuzz_result3 as
        select join_fuzz1.$x, join_fuzz1.$y from join_fuzz1 full outer join join_fuzz2 on join_fuzz1.$x = join_fuzz2.$x and join_fuzz1.$y > join_fuzz2.$y;
"

	# A.3 / B.1 / B.2
        run_bendsql "
        select count() + 1 from (
            (SELECT join_fuzz_result1.$x, join_fuzz_result1.$y FROM join_fuzz_result1
            UNION
            SELECT join_fuzz_result2.$x, join_fuzz_result2.$y FROM join_fuzz_result2)
            EXCEPT
            SELECT join_fuzz_result3.$x, join_fuzz_result3.$y FROM join_fuzz_result3
        );
        select (count(*) > 0)::Int from join_fuzz_result1 where $y is null;
        select (count(*) > 0)::Int from join_fuzz_result2 where $x is null;
        "

	# C.1 - Test FULL OUTER JOIN = LEFT JOIN + RIGHT JOIN - overlapping parts
        # Perform FULL OUTER JOIN with simpler condition for better comparison
        run_bendsql_null "
create or replace table full_outer as
        select coalesce(join_fuzz1.$x, join_fuzz2.$x) as $x, coalesce(join_fuzz1.$y, join_fuzz2.$y) as $y
        from join_fuzz1 full outer join join_fuzz2 on join_fuzz1.$x = join_fuzz2.$x;
"

        # Perform LEFT JOIN
        run_bendsql_null "
create or replace table left_join as
        select join_fuzz1.$x as $x, join_fuzz1.$y as $y from join_fuzz1 left join join_fuzz2 on join_fuzz1.$x = join_fuzz2.$x;
"

        # Perform RIGHT JOIN
        run_bendsql_null "
create or replace table right_join as
        select join_fuzz2.$x as $x, join_fuzz2.$y as $y from join_fuzz1 right join join_fuzz2 on join_fuzz1.$x = join_fuzz2.$x;
"

        # Find overlapping parts (rows that appear in both LEFT and RIGHT joins)
        run_bendsql_null "
create or replace table x_overlap as
        select join_fuzz1.$x as $x, join_fuzz1.$y as $y
        from join_fuzz1 inner join join_fuzz2 on join_fuzz1.$x = join_fuzz2.$x;
"

        # Compare counts: FULL OUTER count should equal (LEFT count + RIGHT count - OVERLAP count)
        run_bendsql "
        select
            (select count(*) from full_outer) = ((select count(*) from left_join) + (select count(*) from right_join) - (select count(*) from x_overlap)) as is_equal
        "
    done
done
