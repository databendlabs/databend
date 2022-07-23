set enable_planner_v2 = 1;

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

-- setup
CREATE TABLE IF NOT EXISTS t(c1 Int, c2 Int );
INSERT INTO t VALUES(1,2);
INSERT INTO t VALUES(3,4);

select 'selection not match, nothing deleted';
delete from t where c1 > 3;
select count(*) = 2 from t;

select 'delete one rows';
delete from t where c1 = 1;
select 'the row should be deleted';
select count(*) = 0 from t where c1 = 1;
select 'other rows should be kept';
select count(*) = 1 from t where c1 <> 1;

select 'deleted unconditionally';
delete from t;
select count(*) = 0 from t;

drop table t all;

-- setup
select 'nullable column cases';
create table t (c Int null);
insert into t values (1),(2),(NULL);

select 'delete with condition "const false"';
delete from t where 1 = 0;
select count(*) = 3 from t;

select 'normal deletion';
delete from t where c = 1;
select 'expects one row deleted';
select count(*) = 2 from t;
select 'expects null valued row kept';
select count(*) = 1 from t where c IS NULL;

select 'delete all null valued rows';
delete from t where c IS NULL;
select 'expects no null valued row kept';
select count(*) = 0 from t where c IS NULL;
select 'expects 1 null valued row kept';
select count(*) = 1 from t where c IS NOT NULL;

select 'deleted unconditionally (with const true)';
delete from t where 1 = 1;
select count(*) = 0 from t;

-- commented out, not sure if we should support this
--insert into t values (1),(2),(NULL);
--select 'deleted with nullary expr now()';
--delete from t where now();
--select count(*) = 0 from t;

drop table t all;

DROP DATABASE db1;
