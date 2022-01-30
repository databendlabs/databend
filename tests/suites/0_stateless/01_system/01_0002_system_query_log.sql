select * from numbers(100) where number > 95;
select count(*) > 0 from system.query_log;


create table tbl_01_0002(a int);

-- insert one row, and one partition
insert into  tbl_01_0002 values(1);
select written_rows from system.query_log where query_text='insert into  tbl_01_0002 values(1)' and written_rows != 0;

-- insert another row and partition
insert into  tbl_01_0002 values(2);
-- totally 2 partitions will be involved, after pruning, only 1 partition will be scanned
select count(*) from tbl_01_0002 where a > 1;
-- let's check it out
select count(1) from system.query_log where query_text='select count(*) from tbl_01_0002 where a > 1' and scan_partitions = 1 and total_partitions = 2;

drop table tbl_01_0002;
