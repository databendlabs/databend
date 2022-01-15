SUDO USE TENANT 'tenant1';
-- database check.
CREATE DATABASE tenant1_db;
SHOW CREATE DATABASE tenant1_db;
DROP DATABASE tenant1_db;

-- table check.
CREATE TABLE tenant1_tbl(a Int);
SHOW CREATE TABLE tenant1_tbl;
DESC tenant1_tbl;
DROP TABLE tenant1_tbl;

-- user.
CREATE USER 'test'@'localhost' IDENTIFIED BY 'password';
ALTER USER 'test'@'localhost' IDENTIFIED BY 'password1';
DROP USER 'test'@'localhost';

-- stage.
CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter='\n') comments='test';
DROP STAGE test_stage;

-- udf
CREATE FUNCTION xy AS (x, y) -> (x + y) / 2;
ALTER FUNCTION xy AS (x, y) -> (x + y) / 3;
SHOW FUNCTION xy;
DROP FUNCTION xy;

SUDO USE TENANT 'tenant2';
-- check
SHOW FUNCTION xy; -- {ErrorCode 4071}
