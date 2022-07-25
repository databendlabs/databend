set enable_planner_v2=1;

DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

select '==Tuple(Boolean, Int64, Float64, String, Date, Timestamp)==';

CREATE TABLE IF NOT EXISTS t1(id Int, t Tuple(Bool, Int64, Float64, String, Date, Timestamp)) Engine = Fuse;

INSERT INTO t1 (id, t) VALUES(1, (true, 100, 12.34, 'abc', '2020-01-01', '2020-01-01 00:00:00')),
                             (2, (false, 200, -25.73, 'xyz', '2022-06-01', '2022-06-01 12:00:00'));

select * from t1;

select '==Nmaed Tuple(a Boolean, b Int64, c Float64, d String, e Date, f Timestamp)==';

CREATE TABLE IF NOT EXISTS t2(id Int, t Tuple(a Bool, b Int64, c Float64, d String, e Date, f Timestamp)) Engine = Fuse;

INSERT INTO t2 (id, t) VALUES(1, (true, 10, 0.5, 'x', '2021-05-01', '2021-05-01 00:00:00')),
                             (2, (false, -10, -0.9, 'y', '2022-10-01', '2022-10-01 12:00:00'));

select * from t2;

DROP DATABASE db1;

set enable_planner_v2=0;