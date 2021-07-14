select number from numbers_mt(10) where number+1 in (2, 3 ,20) order by number;
select number from numbers_mt(10000) where number+1 in (2, 3 ,20, 5000) order by number;
select number from numbers_mt(10) where number+1 not in (2, 3 ,20, 5000) order by number;
