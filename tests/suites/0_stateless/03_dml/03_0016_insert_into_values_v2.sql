set enable_planner_v2 = 1;

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a Int8 null, b UInt32 null, c DateTime null, d String null, e Float64 null) Engine = Fuse;

INSERT INTO t1 (a,b,c,d,e) VALUES(-1, 33, '2021-08-15 10:00:00', 'string1234', 1.4e5),
                                       (101, 67, '2021-11-15 10:00:00', 'string5678', 9.9e-3);

select * from t1;
select sum(a),sum(b) from t1;

CREATE TABLE IF NOT EXISTS t2(a Int8, b DateTime, c String) Engine = Fuse;
INSERT INTO t2 (a,b,c) VALUES(1, '0000-00-00 00:00:00', 'test)');  -- {ErrorCode 1010}

CREATE TABLE IF NOT EXISTS t_str(a Varchar);
INSERT INTO t_str(a) values( 'a"b\"c\'d');
INSERT INTO t_str(a) values( 'a"b\"c\\\'d');
select * from t_str order by a;

DROP DATABASE db1;

set enable_planner_v2 = 0;
