set enable_planner_v2 = 1;

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a Int8 null, b UInt32 null, c Date null, d DateTime null, e String null, f Float64 null) Engine = Fuse;

INSERT INTO t1 (a,b,c,d,e,f) VALUES(-1, 33, '2021-08-15', '2021-08-15 10:00:00', 'string1234', 1.4e5),
                                       (101, 67, '2021-11-15', '2021-11-15 10:00:00', 'string5678', 9.9e-3),
                                       (100, 100, '0000-00-00', '0000-00-00 00:00:00', 'string7890', 12.34),
                                       (0, 0, '0000-00-00', '0000-00-00T00:00:00', 'string7891', 12.35),
                                       (100, 100, '0001-01-01', '0001-01-01 00:00:00', 'stringabcd', 56.78);

select * from t1;
select sum(a),sum(b) from t1;

CREATE TABLE IF NOT EXISTS t_str(a Varchar);
INSERT INTO t_str(a) values( 'a"b\"c\'d');
INSERT INTO t_str(a) values( 'a"b\"c\\\'d');
select * from t_str order by a;

DROP DATABASE db1;

set enable_planner_v2 = 0;
