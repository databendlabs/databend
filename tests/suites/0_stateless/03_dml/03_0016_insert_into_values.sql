DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a Int8 null, b UInt32 null, c DateTime32 null, d String null) Engine = Memory;


INSERT INTO t1 (a,b,c,d) VALUES(-1, 33, '2021-08-15 10:00:00', 'string1234'),
                                       (101, 67, '2021-11-15 10:00:00', 'string5678');
-- INSERT db.table
INSERT INTO db1.t1 (a,b,c,d) VALUES(-2, 66, '2021-08-16 10:00:00', 'string-a'),
                                       (102, 134, '2021-11-16 10:00:00', 'string-b');
INSERT INTO db1.t1(a,b,c,d) VALUES(-3, 99, '2021-08-17 10:00:00', 'string-c'),
                                       (103, 201, '2021-11-17 10:00:00', 'string-d');
-- INSERT table
INSERT INTO TABLE db1.t1(a,b,c,d) VALUES (-4, 132, '2021-08-18 10:00:00', 'string-e'),
                                       (104, 268, '2021-11-18 10:00:00', 'string-f');                                                              

select * from t1;
select sum(a),sum(b) from t1;


DROP DATABASE db1;
