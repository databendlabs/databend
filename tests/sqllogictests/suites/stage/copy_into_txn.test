#################################################################################################################
# test avoiding duplicates when copy stage files into a table
#################################################################################################################

statement ok
create or replace database test_txn_copy;

statement ok
use test_txn_copy;

statement ok
create table t1(c int);

statement ok
begin;

query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV);
----
csv/numbers.csv 18 0 NULL NULL

query I
select count(*) from t1;
----
18

query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV);
----

query I
select count(*) from t1;
----
18

statement ok
commit;

query I
select count(*) from t1;
----
18

query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV);
----

query I
select count(*) from t1;
----
18

#################################################################################################################
# test when txn is aborted, the stage files are not purged
#################################################################################################################

statement ok
create or replace database test_txn_copy_1;

statement ok
use test_txn_copy_1;

statement ok
create table t1(c int);

statement ok
begin;


query
copy into t1 from @data/csv/numbers.csv file_format = (type = CSV) purge = true;
----
csv/numbers.csv 18 0 NULL NULL

statement error 1025
select * from t100;

statement error 4002
select * from t1;

statement ok
commit;

query I
select count(*) from t1;
----
0

statement ok
create table t2(c int);

query
copy into t2 from @data/csv/numbers.csv file_format = (type = CSV);
----
csv/numbers.csv 18 0 NULL NULL

query I
select count(*) from t2;
----
18