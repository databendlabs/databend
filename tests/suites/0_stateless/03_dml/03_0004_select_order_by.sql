set max_threads = 16;
SELECT number, number + 3 FROM numbers_mt (1000) where number > 5 order by number desc limit 3;
SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1 desc, c2 asc;
EXPLAIN SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1, number desc;
SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1, number desc;

create table t1(id int);
insert into t1 select number as id from numbers(10);
select * from t1 order by id asc limit 3,3;
select * from t1 order by id desc limit 3,3;
drop table t1;
