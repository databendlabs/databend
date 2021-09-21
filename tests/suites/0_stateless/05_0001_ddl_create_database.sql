DROP DATABASE IF EXISTS db;

CREATE DATABASE db ENGINE = default;
CREATE TABLE db.t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db';

CREATE DATABASE IF NOT EXISTS db ENGINE = default;
CREATE DATABASE db ENGINE = default; -- {ErrorCode 4001}

DROP DATABASE IF EXISTS db;

CREATE DATABASE db ENGINE = NotExists; -- {ErrorCode 8001}
