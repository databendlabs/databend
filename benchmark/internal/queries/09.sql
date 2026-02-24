-- information_schema.tables exists check (mirrors the reported issue)
select case when exists(select table_name from information_schema.tables where table_schema = 'test' and table_name = 't_1') then 1 else 0 end;
