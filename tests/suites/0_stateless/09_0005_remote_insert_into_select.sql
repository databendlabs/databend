DROP DATABASE IF EXISTS db_09_0005;
CREATE DATABASE db_09_0005;
USE db_09_0005;


create table n1(a uint64);
insert into n1 select number from numbers(10);
insert into n1 select number from numbers(10);
select count(*) from n1;

DROP TABLE n1;
DROP DATABASE db_09_0005;
