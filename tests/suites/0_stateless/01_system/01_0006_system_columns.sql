DROP DATABASE IF EXISTS COLUMNTEST;

CREATE DATABASE COLUMNTEST;

CREATE TABLE COLUMNTEST.A(ID INT, ID2 INT DEFAULT 1, ID3 STRING, ID4 STRING DEFAULT 'ID4');

-- https://github.com/datafuselabs/databend/issues/6042
SELECT lower(database), name, type, default_kind as default_type, default_expression, comment FROM system.columns  WHERE database LIKE 'columntest';

DROP DATABASE COLUMNTEST;
