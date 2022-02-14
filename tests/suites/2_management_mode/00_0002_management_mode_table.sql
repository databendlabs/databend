-- TENANT1
SUDO USE TENANT 'tenant1';

-- setup.
CREATE DATABASE IF NOT EXISTS tenant1_db;
-- TODO: use have some bugs, need fix in another PR
--USE tenant1_db;

--CREATE TABLE tenant1_tbl(a Int);
--SHOW CREATE TABLE tenant1_tbl;
--SHOW TABLES;

--DROP TABLE tenant1_tbl;
DROP DATABASE tenant1_db;

-- TENANT2
SUDO USE TENANT 'tenant2';
--USE tenant1_db; -- {ErrorCode 2301}
