DROP DATABASE IF EXISTS showtable;

CREATE DATABASE showtable;
CREATE TABLE showtable.t1(c1 int) ENGINE = Null;
CREATE TABLE showtable.t2(c1 int) ENGINE = Null;
CREATE TABLE showtable.t3(c1 int) ENGINE = Null;

use showtable;
SHOW TABLES;

SHOW TABLES LIKE 't%';
-- if want to support SHOW TABLES LIKE "t2" link to this pr:
-- https://github.com/datafuse-extras/sqlparser-rs/pull/34/files
SHOW TABLES LIKE 't2';
SHOW TABLES LIKE 't';

SHOW TABLES WHERE table_name LIKE 't%';
SHOW TABLES WHERE table_name = 't%' AND 1 = 0;
SHOW TABLES WHERE table_name = 't2' OR 1 = 1;
SHOW TABLES WHERE table_name = 't2' AND 1 = 1;

USE default;
SHOW TABLES FROM showtables WHERE table_name LIKE 't%';
SHOW TABLES FROM showtables WHERE table_name = "t%" AND 1 = 0;
SHOW TABLES FROM showtables WHERE table_name = 't2' OR 1 = 1;
SHOW TABLES FROM showtables WHERE table_name = 't2' AND 1 = 1;
DROP DATABASE showtable;
