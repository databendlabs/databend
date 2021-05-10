-- https://github.com/datafuselabs/datafuse/issues/492
SELECT number ,number-1 , number*100 , 1> 100 ,1 < 10 FROM numbers_mt (10) order by number;

