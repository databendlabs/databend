DROP DATABASE IF EXISTS db;

CREATE DATABASE db ENGINE = Local;
CREATE TABLE db.t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db';

CREATE DATABASE IF NOT EXISTS db ENGINE = Local;
CREATE DATABASE db ENGINE = Local; -- {ErrorCode 3}

DROP DATABASE IF EXISTS db;

CREATE DATABASE db ENGINE = NotExists;
