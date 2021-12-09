set max_threads=16;
truncate table system.query_log;
select * from numbers(100) where number > 95;
select count(0) > 0 from system.query_log;

