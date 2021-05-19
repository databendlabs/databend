set max_threads = 16;
SELECT * FROM numbers_mt (3) where number > 5;
SELECT * FROM numbers_mt (3) where number > 1;
SELECT * FROM numbers_mt (3) where 1=2;
SELECT number as c1, (number+1) as c2 FROM numbers_mt (3) where number+1>1;
EXPLAIN SELECT number as c1, (number+1) as c2 FROM numbers_mt (3) where number >1;
SELECT number as c1, (number+1) as c2 FROM numbers_mt (3) where number >1;
