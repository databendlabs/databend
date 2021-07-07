select number from numbers_mt(10) where number+1 in (2, 3 ,20);
select number from numbers_mt(10000) where number+1 in (2, 3 ,20, 5000);
