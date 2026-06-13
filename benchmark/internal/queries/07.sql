-- db-only filter (slow path, no table name)
select name from system.tables where database = 'test';
