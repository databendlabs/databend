DROP DATABASE IF EXISTS db12_0002;
CREATE DATABASE db12_0002;
USE db12_0002;

CREATE TABLE t(c1 int);
create view v_t as select * from t;
show full tables from db12_0002;
show full views from db12_0002;

DROP TABLE t;

show tables history like 't';

DROP database db12_0002;
