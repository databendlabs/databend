DROP DATABASE IF EXISTS db;

CREATE DATABASE db;
CREATE TABLE db.t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'db';

CREATE DATABASE IF NOT EXISTS db;
CREATE DATABASE db; -- {ErrorCode 2301}

DROP DATABASE IF EXISTS db;

CREATE DATABASE system; -- {ErrorCode 2301}
DROP DATABASE system; -- {ErrorCode 1002}
