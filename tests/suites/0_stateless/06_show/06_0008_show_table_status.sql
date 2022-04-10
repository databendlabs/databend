DROP DATABASE IF EXISTS showtabstat;
CREATE DATABASE showtabstat;
CREATE TABLE showtabstat.t1(c1 int) ENGINE = Null;
CREATE TABLE showtabstat.t2(c1 int) ENGINE = Null;
CREATE TABLE showtabstat.t3(c1 int) ENGINE = Null;

USE showtabstat;
SHOW TABLE STATUS;

SHOW TABLE STATUS LIKE 't%';
SHOW TABLE STATUS LIKE 't2';
SHOW TABLE STATUS LIKE 't';

SHOW TABLE STATUS WHERE name LIKE 't%';
SHOW TABLE STATUS WHERE name = 't%' AND 1 = 0;
SHOW TABLE STATUS WHERE name = 't2' OR 1 = 1;
SHOW TABLE STATUS WHERE name = 't2' AND 1 = 1;

USE default;
SHOW TABLE STATUS FROM showtabstat WHERE name LIKE 't%';
SHOW TABLE STATUS FROM showtabstat WHERE name = 't%' AND 1 = 0;
SHOW TABLE STATUS FROM showtabstat WHERE name = 't2' OR 1 = 1;
SHOW TABLE STATUS FROM showtabstat WHERE name = 't2' AND 1 = 1;

DROP DATABASE IF EXISTS showtabstat;
