-- system.columns with precise filter
select name from system.columns where database = 'test' and `table` = 't_1';
