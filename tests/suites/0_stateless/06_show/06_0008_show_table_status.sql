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

SHOW TABLE STATUS WHERE Name LIKE 't%';
SHOW TABLE STATUS WHERE Name = 't%' AND 1 = 0;
SHOW TABLE STATUS WHERE Name = 't2' OR 1 = 1;
SHOW TABLE STATUS WHERE Name = 't2' AND 1 = 1;

USE default;
SHOW TABLE STATUS FROM showtabstat WHERE Name LIKE 't%';
SHOW TABLE STATUS FROM showtabstat WHERE Name = 't%' AND 1 = 0;
SHOW TABLE STATUS FROM showtabstat WHERE Name = 't2' OR 1 = 1;
SHOW TABLE STATUS FROM showtabstat WHERE Name = 't2' AND 1 = 1;



CREATE TABLE showtabstat.t4(c1 int);
SHOW TABLE STATUS FROM showtabstat WHERE Name = 't4';
insert into showtabstat.t4 values(1);
SHOW TABLE STATUS FROM showtabstat WHERE Name = 't4';

SHOW TABLE STATUS FROM showtabstat WHERE engine = 'test';
SHOW TABLE STATUS FROM showtabstat WHERE Create_time = 'test';

DROP DATABASE IF EXISTS showtabstat;
