set max_threads=1;
SELECT (number+1) as c1, max(number) as c2 FROM numbers_mt(10) where c2<10 group by c1%3 having c2>1 order by c1 desc, c2 asc;