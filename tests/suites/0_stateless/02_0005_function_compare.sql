-- https://github.com/datafuselabs/datafuse/issues/492
SELECT number ,number-1 , number*100 , 1> 100 ,1 < 10 FROM numbers_mt (10) order by number;

-- between
select number from numbers_mt(10) where number  not between 4 + 0.1  and 8 - 0.1  order by number;
select number from numbers_mt(10) where number   between 4 + 0.1  and 8 - 0.1  order by number;

-- like
select * from system.databases where name like '%sys%';
select * from system.databases where name like '_ef_ul_';

-- not like
select * from system.databases where name not like '%sys%' order by name;
select * from system.databases where name not like '_ef_ul_' order by name;
