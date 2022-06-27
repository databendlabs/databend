DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

select '==Array(UInt8)==';

CREATE TABLE IF NOT EXISTS t1(id Int, arr Array(UInt8)) Engine = Fuse;

INSERT INTO t1 (id, arr) VALUES(1, [1,2,3]), (2, [254,255]);

select * from t1;
select arr[0], arr[1] from t1;

select '==Array(UInt16)==';

CREATE TABLE IF NOT EXISTS t2(id Int, arr Array(UInt16)) Engine = Fuse;

INSERT INTO t2 (id, arr) VALUES(1, [1,2,3]), (2, [65534,65535]);

select * from t2;
select arr[0], arr[1] from t2;

select '==Array(UInt32)==';

CREATE TABLE IF NOT EXISTS t3(id Int, arr Array(UInt32)) Engine = Fuse;

INSERT INTO t3 (id, arr) VALUES(1, [1,2,3]), (2, [4294967294,4294967295]);

select * from t3;
select arr[0], arr[1] from t3;

select '==Array(UInt64)==';

CREATE TABLE IF NOT EXISTS t4(id Int, arr Array(UInt64)) Engine = Fuse;

INSERT INTO t4 (id, arr) VALUES(1, [1,2,3]), (2, [18446744073709551614,18446744073709551615]);

select * from t4;
select arr[0], arr[1] from t4;

select '==Array(Int8)==';

CREATE TABLE IF NOT EXISTS t5(id Int, arr Array(Int8)) Engine = Fuse;

INSERT INTO t5 (id, arr) VALUES(1, [1,2,3]), (2, [-128,127]);

select * from t5;
select arr[0], arr[1] from t5;

select '==Array(Int16)==';

CREATE TABLE IF NOT EXISTS t6(id Int, arr Array(Int16)) Engine = Fuse;

INSERT INTO t6 (id, arr) VALUES(1, [1,2,3]), (2, [-32768,32767]);

select * from t6;
select arr[0], arr[1] from t6;

select '==Array(Int32)==';

CREATE TABLE IF NOT EXISTS t7(id Int, arr Array(Int32)) Engine = Fuse;

INSERT INTO t7 (id, arr) VALUES(1, [1,2,3]), (2, [-2147483648,2147483647]);

select * from t7;
select arr[0], arr[1] from t7;

select '==Array(Int64)==';

CREATE TABLE IF NOT EXISTS t8(id Int, arr Array(Int64)) Engine = Fuse;

INSERT INTO t8 (id, arr) VALUES(1, [1,2,3]), (2, [-9223372036854775808,9223372036854775807]);

select * from t8;
select arr[0], arr[1] from t8;

select '==Array(Float32)==';

CREATE TABLE IF NOT EXISTS t9(id Int, arr Array(Float32)) Engine = Fuse;

INSERT INTO t9 (id, arr) VALUES(1, [1.1,1.2,1.3]), (2, [-1.1,-1.2,-1.3]);

select * from t9;
select arr[0], arr[1] from t9;

select '==Array(Float64)==';

CREATE TABLE IF NOT EXISTS t10(id Int, arr Array(Float64)) Engine = Fuse;

INSERT INTO t10 (id, arr) VALUES(1, [1.1,1.2,1.3]), (2, [-1.1,-1.2,-1.3]);

select * from t10;
select arr[0], arr[1] from t10;

select '==Array(Boolean)==';

CREATE TABLE IF NOT EXISTS t11(id Int, arr Array(Bool)) Engine = Fuse;

INSERT INTO t11 (id, arr) VALUES(1, [true, true]), (2, [false, false]), (3, [true, false]), (4, [false, true]);

select * from t11;
select arr[0], arr[1] from t11;

select '==Array(Date)==';

CREATE TABLE IF NOT EXISTS t12(id Int, arr Array(Date)) Engine = Fuse;

INSERT INTO t12 (id, arr) VALUES(1, ['2021-01-01', '2022-01-01']), (2, ['1990-12-01', '2030-01-12']);
INSERT INTO t12 (id, arr) VALUES(3, ['1000000-01-01', '2000000-01-01']); -- {ErrorCode 1010}

select * from t12;
select arr[0], arr[1] from t12;

select '==Array(Timestamp)==';

CREATE TABLE IF NOT EXISTS t13(id Int, arr Array(Timestamp)) Engine = Fuse;

INSERT INTO t13 (id, arr) VALUES(1, ['2021-01-01 01:01:01', '2022-01-01 01:01:01']), (2, ['1990-12-01 10:11:12', '2030-01-12 22:00:00']);
INSERT INTO t13 (id, arr) VALUES(3, ['1000000-01-01 01:01:01', '2000000-01-01 01:01:01']); -- {ErrorCode 1010}

select * from t13;
select arr[0], arr[1] from t13;

select '==Array(String)==';

CREATE TABLE IF NOT EXISTS t14(id Int, arr Array(String)) Engine = Fuse;

INSERT INTO t14 (id, arr) VALUES(1, ['aa', 'bb']), (2, ['cc', 'dd']);

select * from t14;
select arr[0], arr[1] from t14;

select '==Array(String) Nullable==';

CREATE TABLE IF NOT EXISTS t15(id Int, arr Array(String) Null) Engine = Fuse;

INSERT INTO t15 (id, arr) VALUES(1, ['aa', 'bb']), (2, ['cc', 'dd']), (3, null), (4, ['ee', 'ff']);

select * from t15;
select arr[0], arr[1] from t15;

select '==Array(Int64) Nullable==';

CREATE TABLE IF NOT EXISTS t16(id Int, arr Array(Int64) Null) Engine = Fuse;

INSERT INTO t16 (id, arr) VALUES(1, [1,2,3,4]), (2, [5,6,7,8]), (3, null);

select * from t16;
select arr[0], arr[1] from t16;
select arr[0], arr[1] from t16 where arr[1] = 6 order by arr[2] desc;

DROP DATABASE db1;
