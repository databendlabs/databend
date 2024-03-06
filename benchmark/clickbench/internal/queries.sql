-- test system table query speed
select name from system.tables union all select name from system.columns union all select name from system.databases union all select name from system.functions ignore_result;
show grants for a;
show grants for role role1;
