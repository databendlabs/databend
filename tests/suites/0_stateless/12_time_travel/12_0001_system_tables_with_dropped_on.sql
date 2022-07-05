DROP DATABASE IF EXISTS db_12_0001;
CREATE DATABASE db_12_0001;
USE db_12_0001;

CREATE TABLE t(c1 int);
select "system.tables should contain the newly created table";
SELECT COUNT(*)=1 from system.tables where name = 't' and database = 'db_12_0001';
select "system.tables_with_history should contain the newly created table, and dropped_on should be NULL";
SELECT COUNT(*)=1 from system.tables_with_history where name = 't' and database = 'db_12_0001' and dropped_on = 'NULL';

DROP TABLE t;
select "system.tables should NOT contain the dropped table";
SELECT COUNT(*)=0 from system.tables where name = 't' and database = 'db_12_0001';
select "system.tables_with_history should contain the dropped table, and dropped_on should NOT be NULL";
SELECT COUNT(*)=1 from system.tables_with_history where name = 't' and database = 'db_12_0001' and dropped_on != 'NULL';

DROP database db_12_0001;
