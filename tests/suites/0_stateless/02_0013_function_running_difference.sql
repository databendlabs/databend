drop table if exists runing_difference_test;

create table runing_difference_test (a Int8, b Int32, c Int64, d varchar) engine=Memory;
insert into runing_difference_test values (1, 1, 1, 'a'),(3, 3, 3, 'b'),(5, 5, 5, 'c'),(10, 10, 10, 'd');

select a, runningDifference(a), b, runningDifference(b), c, runningDifference(c), runningDifference(10) from runing_difference_test;
select d, runningDifference(d) from runing_difference_test;

DROP TABLE IF EXISTS runing_difference_test;