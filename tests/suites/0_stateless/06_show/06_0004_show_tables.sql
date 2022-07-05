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

SHOW TABLES WHERE Tables_in_showtable LIKE 't%';
SHOW TABLES WHERE Tables_in_showtable = 't%' AND 1 = 0;
SHOW TABLES WHERE Tables_in_showtable = 't2' OR 1 = 1;
SHOW TABLES WHERE Tables_in_showtable = 't2' AND 1 = 1;

USE default;
SHOW TABLES FROM showtables WHERE Tables_in_showtables LIKE 't%';
SHOW TABLES FROM showtables WHERE Tables_in_showtables = "t%" AND 1 = 0;
SHOW TABLES FROM showtables WHERE Tables_in_showtables = 't2' OR 1 = 1;
SHOW TABLES FROM showtables WHERE Tables_in_showtables = 't2' AND 1 = 1;
DROP DATABASE showtable;
