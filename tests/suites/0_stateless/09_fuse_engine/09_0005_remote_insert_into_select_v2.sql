set enable_planner_v2 = 1;

DROP DATABASE IF EXISTS db_09_0005;
CREATE DATABASE db_09_0005;
USE db_09_0005;


create table n1(a uint64);
insert into n1 select number from numbers(10);
insert into n1 select number from numbers(10);
select count(*) from n1;

DROP TABLE n1;


select 'column with default value Null';
CREATE TABLE t ( `id` BIGINT UNSIGNED NULL, `business_id` BIGINT UNSIGNED NULL, `col3` BIGINT UNSIGNED NULL  );
select 'edge case1';
insert into t(id) select * from numbers(112);
select 'edge case2';
insert into t(id) select * from numbers(113);
select 'edge case3';
insert into t(id) select * from numbers(20000);
select 'checking count: count(*) = (112 + 113 + 20000)';
select count(*) = (112 + 113 + 20000) from t;
select 'checking nullable column with non-null values';
select sum(id) from t;

select 'checking nullable columns with all null values: business_id';
select count(*) from t where IS_NOT_NULL(business_id);
select sum(col3) from t;
select 'checking nullable columns with all null values: cols';
select count(*) from t where IS_NOT_NULL(col3);
select sum(business_id) from t;

DROP TABLE t;

DROP DATABASE db_09_0005;

set enable_planner_v2 = 0;