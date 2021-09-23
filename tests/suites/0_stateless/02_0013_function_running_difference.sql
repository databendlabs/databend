drop table if exists runing_difference_test;

create table runing_difference_test (a Int8, b Int32, c Int64) engine=Memory;
insert into runing_difference_test values (1, 1, 1),(3, 3, 3),(5, 5, 5),(10, 10, 10);

select a, runningDifference(a), b, runningDifference(b), c, runningDifference(c) from runing_difference_test;

DROP TABLE IF EXISTS runing_difference_test;