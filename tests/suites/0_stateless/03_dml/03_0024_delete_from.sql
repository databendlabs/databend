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

DROP DATABASE db1;
