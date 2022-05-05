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

set enable_planner_v2 = 0;