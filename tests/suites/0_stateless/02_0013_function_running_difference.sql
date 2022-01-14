drop table if exists runing_difference_test;

create table runing_difference_test (a Int8, b Int32, c Int64, d varchar, e Float32, f Float64) engine=Memory;
insert into runing_difference_test values (1, 1, 1, 'a', 1, 1),(3, 3, 3, 'b', 3, 3),(5, 5, 5, 'c', 5, 5),(10, 10, 10, 'd', 10, 10);

select runningDifference(a), runningDifference(b), runningDifference(c), runningDifference(e), runningDifference(10) from runing_difference_test;
select d, runningDifference(d) from runing_difference_test; -- {ErrorCode 1007}

DROP TABLE IF EXISTS runing_difference_test;
