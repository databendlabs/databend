-- test system table query speed
select name
from system.tables
union all
select name
from system.columns
union all
select name
from system.databases
union all
select name
from system.functions ignore_result;
select name from system.tables where name='t_1';
select name from system.tables where name in ('t_1', 't_2');
