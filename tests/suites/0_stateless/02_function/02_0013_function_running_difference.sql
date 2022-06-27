drop table if exists running_difference_test;

create table running_difference_test (a Int8, b Int32, c Int64, d varchar, e Float32, f Float64) Engine = Fuse;
insert into running_difference_test values (1, 1, 1, 'a', 1, 1),(3, 3, 3, 'b', 3, 3),(5, 5, 5, 'c', 5, 5),(10, 10, 10, 'd', 10, 10);

select running_difference(a), running_difference(b), running_difference(c), running_difference(e), running_difference(10) from running_difference_test;
select d, running_difference(d) from running_difference_test; -- {ErrorCode 1007}

DROP TABLE IF EXISTS running_difference_test;
