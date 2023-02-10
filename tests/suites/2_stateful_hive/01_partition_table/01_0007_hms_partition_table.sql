-- test prewhere, read all data
select c_nation2,c_region2 from hive.default.customer_p2 where c_nation like 'CHIN%' order by c_nation2;
