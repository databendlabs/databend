-- information_schema.tables with schema filter (mirrors the reported issue)
select table_name from information_schema.tables where table_schema = 'test' and engine not like '%VIEW%';
