select 1, sum(number) from numbers_mt(1000000);
select count(*) = count(1) from numbers(1000);
select count(1) from numbers(1000);
select sum(3) from numbers(1000);
select count(null), min(null), sum(null), avg(null) from numbers(1000);
select sum(a), sum(b), sum(c), sum(e) from ( select (number % 8)::UInt64 as a,(number % 9)::UInt64 as b,(number % 10)::UInt64  as c, count() as e from numbers(100) group by a ,b,c);
