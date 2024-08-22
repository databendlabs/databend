#################################################################################################################
# test avoiding duplicates when copy stage files into a table
#################################################################################################################

onlyif mysql
statement ok
create or replace database test_txn_copy;

onlyif mysql
statement ok
use test_txn_copy;

onlyif mysql
statement ok
create temp table t1(c int);

onlyif mysql
statement ok
begin;

onlyif mysql
query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV);
----
csv/numbers.csv 18 0 NULL NULL

onlyif mysql
query I
select count(*) from t1;
----
18

onlyif mysql
query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV);
----

onlyif mysql
query I
select count(*) from t1;
----
18

onlyif mysql
statement ok
commit;

onlyif mysql
query I
select count(*) from t1;
----
18

onlyif mysql
query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV);
----

onlyif mysql
query I
select count(*) from t1;
----
18

#################################################################################################################
# test when txn is aborted, the stage files are not purged
#################################################################################################################

onlyif mysql
statement ok
create or replace database test_txn_copy_1;

onlyif mysql
statement ok
use test_txn_copy_1;

onlyif mysql
statement ok
create temp table t1(c int);

onlyif mysql
statement ok
begin;

onlyif mysql
query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV) purge = true;
----
csv/numbers.csv 18 0 NULL NULL

onlyif mysql
statement error 1025
select * from t100;

onlyif mysql
statement error 4002
select * from t1;

onlyif mysql
statement ok
commit;

onlyif mysql
query I
select count(*) from t1;
----
0

onlyif mysql
statement ok
create temp table t2(c int);

onlyif mysql
query
copy into t2 from @data/csv/numbers.csv file_format = (type = CSV);
----
csv/numbers.csv 18 0 NULL NULL

onlyif mysql
query I
select count(*) from t2;
----
18