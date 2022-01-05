select * from numbers(100) where number > 95;
select count(*) > 0 from system.query_log;
create table tbl_01_0002(a int);

-- insert one row, and one partition
insert into  tbl_01_0002 values(1);
select written_rows from system.query_log where query_text='insert into  tbl_01_0002 values(1)' and written_rows != 0;

-- insert another row and partition
insert into  tbl_01_0002 values(2);
-- two rows and two partitions will be scanned
select count(*) from tbl_01_0002;
select scan_rows, scan_partitions from system.query_log where query_text='select count(*) from tbl_01_0002' and scan_partitions != 0;

drop table tbl_01_0002;
