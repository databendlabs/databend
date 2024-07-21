statement ok
set transaction_time_limit_in_hours=0;

statement ok
create or replace table t09_0042 (a int);

statement ok
insert into t09_0042 values (1);

statement error 4003
insert into t09_0042 values (1);

statement ok
drop stage if exists s09_0042;

statement ok
create stage s09_0042 FILE_FORMAT = (TYPE = CSV);

statement error 4003
copy into t09_0042 from @s09_0042;

statement ok
set transaction_time_limit_in_hours=24;
