#################################################################################################################
# test avoiding duplicates when copy stage files into a table
#################################################################################################################

statement ok
create or replace database test_txn_copy;

statement ok
use test_txn_copy;

statement ok
CREATE TABLE t(c int);

statement ok
insert into t values (1478);

statement ok
DROP STAGE IF EXISTS s_txn_copy;

statement ok
CREATE STAGE s_txn_copy;

statement ok
copy into @s_txn_copy from t;

statement ok
create table t1(c int);

onlyif mysql
statement ok
begin;

query I
select * from @s_txn_copy;
----
1478

statement ok
copy into t1 from @s_txn_copy;

query I
select * from t1;
----
1478

statement ok
copy into t1 from @s_txn_copy;

query I
select * from t1;
----
1478

onlyif mysql
statement ok
commit;

query I
select * from t1;
----
1478

statement ok
copy into t1 from @s_txn_copy;

query I
select * from t1;
----
1478

#################################################################################################################
# test when txn is aborted, the stage files are not purged
#################################################################################################################

statement ok
create or replace database test_txn_copy_1;

statement ok
use test_txn_copy_1;

statement ok
CREATE TABLE t(c int);

statement ok
insert into t values (1478);

statement ok
DROP STAGE IF EXISTS s_txn_copy_1;

statement ok
CREATE STAGE s_txn_copy_1;

statement ok
copy into @s_txn_copy_1 from t;

statement ok
create table t1(c int);

onlyif mysql
statement ok
begin;

onlyif mysql
statement ok
copy into t1 from @s_txn_copy_1 purge = true;

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

query I
select * from @s_txn_copy_1;
----
1478

