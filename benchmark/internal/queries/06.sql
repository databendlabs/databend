-- precise filter: db + table name (optimized path)
select name from system.tables where database = 'test' and name = 't_1';
