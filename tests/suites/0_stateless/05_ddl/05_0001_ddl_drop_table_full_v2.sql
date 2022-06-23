set enable_planner_v2 = 1;

DROP DATABASE IF EXISTS db_13_0001;
CREATE DATABASE db_13_0001;
USE db_13_0001;

-- new SQL syntax that not backward compatible
CREATE TABLE t(c1 int) ENGINE = Null;
DROP TABLE t ALL;

CREATE TABLE t(c1 int) ENGINE = Fuse;
DROP TABLE t ALL;

DROP database db_13_0001;

set enable_planner_v2 = 0;
