DROP DATABASE IF EXISTS db;

CREATE DATABASE db;
CREATE TABLE db.t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db';

CREATE DATABASE IF NOT EXISTS db;
CREATE DATABASE db; -- {ErrorCode 2301}

DROP DATABASE IF EXISTS db;

CREATE DATABASE system; -- {ErrorCode 2301}
DROP DATABASE system; -- {ErrorCode 1002}

CREATE DATABASE db.t; -- {ErrorCode 1005}

DROP SCHEMA IF EXISTS db;

CREATE SCHEMA db;
CREATE TABLE db.t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db';

CREATE SCHEMA IF NOT EXISTS db;
CREATE SCHEMA db; -- {ErrorCode 2301}

DROP SCHEMA IF EXISTS db;

CREATE SCHEMA system; -- {ErrorCode 2301}
DROP SCHEMA system; -- {ErrorCode 1002}

CREATE SCHEMA db.t; -- {ErrorCode 1005}
