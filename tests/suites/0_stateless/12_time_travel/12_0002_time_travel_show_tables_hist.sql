DROP DATABASE IF EXISTS db12_0002;
CREATE DATABASE db12_0002;
USE db12_0002;

CREATE TABLE t(c1 int);
DROP TABLE t;

show tables history like 't';

DROP database db12_0002;
