-- test table data size
create table temp (col uint8);
insert into temp values(1);
select data_size, data_compressed_size from system.tables where name = 'temp';
drop table temp;
