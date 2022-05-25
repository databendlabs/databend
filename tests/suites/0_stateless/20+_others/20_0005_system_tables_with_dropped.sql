DROP DATABASE IF EXISTS db_20_0005;
CREATE DATABASE db_20_0005;
USE db_20_0005;

select "column 'dropped_on' of system.tables should work";
CREATE TABLE t(c1 int);
SELECT COUNT(1) from system.tables where name = 't' and database = 'db_20_0005' and dropped_on = 'NULL';

select "dropped table has history";
DROP TABLE t;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db_20_0005' and dropped_on != 'NULL';

DROP database db_20_0005;