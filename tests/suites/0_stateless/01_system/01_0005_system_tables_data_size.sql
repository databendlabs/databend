create table temp as select * from numbers(10);
select data_size, data_compressed_size from system.tables where name = 'temp';
drop table temp;
