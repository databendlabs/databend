set enable_planner_v2 = 1;

DROP TABLE IF EXISTS t;

CREATE TABLE t(c1 int) ENGINE = Null;

DROP TABLE t;
DROP TABLE IF EXISTS t;
DROP TABLE t; -- {ErrorCode 1025}

DROP TABLE system.tables; -- {ErrorCode 1002}

set enable_planner_v2 = 0;
