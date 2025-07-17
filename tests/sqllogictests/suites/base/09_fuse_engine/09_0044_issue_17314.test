statement ok
create or replace database issue_17314;

statement ok
use issue_17314

statement ok
set enable_analyze_histogram=1;

statement ok
create or replace table t1(a string, biz_date1 string);

statement ok
insert into t1 values('1', '11');

statement ok
alter table t1 rename BIZ_date1 to BIZ_DATE;

statement ok
analyze table t1;

statement ok
insert into t1 values('2', '22');

statement ok
insert into t1 values('3', '33');

statement ok
alter table t1 rename BIZ_DATE to b;

statement ok
analyze table t1;

query IIT
select * from fuse_statistic('issue_17314', 't1') order by column_name;
----
a 3 [bucket id: 0, min: "1", max: "1", ndv: 1.0, count: 1.0], [bucket id: 1, min: "2", max: "2", ndv: 1.0, count: 1.0], [bucket id: 2, min: "3", max: "3", ndv: 1.0, count: 1.0]
b 3 [bucket id: 0, min: "11", max: "11", ndv: 1.0, count: 1.0], [bucket id: 1, min: "22", max: "22", ndv: 1.0, count: 1.0], [bucket id: 2, min: "33", max: "33", ndv: 1.0, count: 1.0]

statement ok
drop table t1 all;

statement ok
drop database issue_17314;
