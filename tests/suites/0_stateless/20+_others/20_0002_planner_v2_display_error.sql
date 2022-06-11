set enable_planner_v2 = 1;

create table t(a int, b int);
select aa from t;
select *;
create table t1(a int);
select a as b from t1 intersect select b from t1 order by a;