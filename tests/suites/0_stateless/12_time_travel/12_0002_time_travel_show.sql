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

DROP SEQUENCE if exists seq;
DROP SEQUENCE if exists seq1;
CREATE SEQUENCE seq;
CREATE SEQUENCE seq1;
select interval, current, comment from show_sequences();
desc sequence seq;
DROP SEQUENCE if exists seq;
DROP SEQUENCE if exists seq1;
