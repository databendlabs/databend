DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a Int8 null, b UInt32 null, c DateTime null, d String null, e Float64 null) Engine = Memory;


INSERT INTO t1 (a,b,c,d,e) VALUES(-1, 33, '2021-08-15 10:00:00', 'string1234', 1.4e5),
                                       (101, 67, '2021-11-15 10:00:00', 'string5678', 9.9e-3);

select * from t1;
select sum(a),sum(b) from t1;


CREATE TABLE IF NOT EXISTS t_str(a Varchar);
INSERT INTO t_str(a) values( 'a"b\"c\'d');
INSERT INTO t_str(a) values( 'a"b\"c\\\'d');
select * from t_str order by a;

drop table t_str;

DROP DATABASE db1;
