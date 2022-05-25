DROP DATABASE IF EXISTS db_20_0007;
CREATE DATABASE db_20_0007;
USE db_20_0007;

CREATE TABLE t(c1 int);
DROP TABLE t;

show tables history like 't';

DROP database db_20_0007;
