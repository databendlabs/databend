-- test system table query speed
select * from system.tables  where database='test' ignore_result;
select * from system.columns  where database='test' ignore_result;