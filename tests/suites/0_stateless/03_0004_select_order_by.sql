set max_threads = 16;
SELECT number, number + 3 FROM numbers_mt (1000) where number > 5 order by number desc limit 3;
SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1 desc, c2 asc;
EXPLAIN SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1, number desc;
SELECT number%3 as c1, number%2 as c2 FROM numbers_mt (10) order by c1, number desc;
