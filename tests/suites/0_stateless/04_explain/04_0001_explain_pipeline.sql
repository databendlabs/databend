set max_threads=8;
explain pipeline select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1;
explain pipeline select avg(number) c   from numbers(100000) group by number % 1000 having c > 100 limit 1;
