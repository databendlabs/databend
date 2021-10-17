DROP DATABASE IF EXISTS db;

CREATE DATABASE db ENGINE = default;
CREATE TABLE db.t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db';

CREATE DATABASE IF NOT EXISTS db ENGINE = default;
CREATE DATABASE db ENGINE = default; -- {ErrorCode 4001}

DROP DATABASE IF EXISTS db;

-- arg engine is just ignored: https://github.com/datafuselabs/databend/issues/2205
CREATE DATABASE db2 ENGINE = NotExists;
CREATE TABLE db2.t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db2';
