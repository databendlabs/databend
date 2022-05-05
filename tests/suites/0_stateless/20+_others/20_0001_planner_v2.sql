set enable_planner_v2 = 1;

select * from numbers(10);

-- Comparison expressions
select * from numbers(10) where number between 1 and 9 and number > 2 and number < 8 and number is not null and number = 5 and number >= 5 and number <= 5;

-- Cast expression
select * from numbers(10) where cast(number as string) = '5';

-- Binary operator
select (number + 1 - 2) * 3 / 4 from numbers(1);

-- Functions
select sin(cos(number)) from numbers(1);

-- In list
select * from numbers(5) where number in (1, 3);

-- Aggregator operator
create table t(a int, b int);
insert into t values(1, 2), (2, 3), (3, 4);
select sum(a) from t group by a;
select sum(a) from t;
select count(a) from t group by a;
select count(a) from t;
select count() from t;
select count() from t group by a;
select count(1) from t;
select count(1) from t group by a;
select count(*) from t;
drop table t;
set enable_planner_v2 = 0;