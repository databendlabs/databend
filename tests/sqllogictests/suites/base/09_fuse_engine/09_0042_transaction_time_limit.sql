statement ok
create or replace table t09_0042 (a int) cluster by (a);

statement ok
insert into t09_0042 values (1),(4);

statement ok
insert into t09_0042 values (2),(3);

statement ok
insert into t09_0042 values (5);

statement ok
set data_retention_time_in_days=0;

statement error 4003
alter table t09_0042 recluster;

statement error 4003
optimize table t09_0042 compact;

statement error 4003
optimize table t09_0042 compact segment;

statement error 4003
insert into t09_0042 values (1);

statement ok
drop stage if exists s09_0042;

statement ok
create stage s09_0042 FILE_FORMAT = (TYPE = CSV);

statement ok
copy into @s09_0042 from t09_0042;

statement error 4003
copy into t09_0042 from @s09_0042;

statement error 4003
delete from t09_0042;

statement error 4003
update t09_0042 set a = 2;

statement error 4003
merge into t09_0042 using (select 1 as a) as s on t09_0042.a = s.a when matched then update set a = 2;

statement error 4003
replace into t09_0042 on (a) values (1);

statement error 4003
insert all into t09_0042 into t09_0042 select * from t09_0042;

statement ok
begin;

statement error 4003
insert into t09_0042 values (1);

statement ok
commit;

query I
select * from t09_0042 order by a;
----
1
2
3
4
5

statement ok
set data_retention_time_in_days=1;
