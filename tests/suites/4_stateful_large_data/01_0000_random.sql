create table oracle (a int, b timestamp, c String) Engine = Random;
create stage oracle;
copy into @oracle from (select * from oracle limit 10000) file_format=(type=parquet) max_file_size=8;
create table target (a int, b timestamp, c String);
copy into target from @oracle file_format=(type=parquet);
select count(*) from target;
