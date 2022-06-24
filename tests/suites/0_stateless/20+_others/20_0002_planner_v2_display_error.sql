set enable_planner_v2 = 1;

create table t(a int, b int);
select aa from t;
select *;
create table t1(a int);
create table t2(a int);
select a as b from t1 intersect select a as b from t2 order by a;