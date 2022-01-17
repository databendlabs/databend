-- TENANT1
SUDO USE TENANT 'tenant1';
-- database check.
CREATE DATABASE tenant1_db;
SHOW CREATE DATABASE tenant1_db;
SHOW DATABASES;

-- table check.
USE tenant1_db;
CREATE TABLE tenant1_tbl(a Int);
SHOW CREATE TABLE tenant1_tbl;
SHOW TABLES;
DESC tenant1_tbl;

-- table insert deined.
INSERT INTo tenant1_tbl VALUES(1); -- {ErrorCode 1062}

DROP TABLE tenant1_tbl;
DROP DATABASE tenant1_db;

-- user check.
CREATE USER 'test'@'localhost' IDENTIFIED BY 'password';
ALTER USER 'test'@'localhost' IDENTIFIED BY 'password1';
DROP USER 'test'@'localhost';

-- stage check.
CREATE STAGE test_stage url='s3://load/files/' credentials=(access_key_id='1a2b3c' secret_access_key='4x5y6z') file_format=(FORMAT=CSV compression=GZIP record_delimiter='\n') comments='test';
DROP STAGE test_stage;

-- udf check.
CREATE FUNCTION xy AS (x, y) -> (x + y) / 2;
ALTER FUNCTION xy AS (x, y) -> (x + y) / 3;
SHOW FUNCTION xy;
DROP FUNCTION xy;


-- TENANT2
SUDO USE TENANT 'tenant2';
SHOW DATABASES;
SHOW TABLES;

-- check
SHOW FUNCTION xy; -- {ErrorCode 2602}
