DROP TABLE IF EXISTS t;

CREATE TABLE t(c1 int) ENGINE = Null;

DROP TABLE t;
DROP TABLE IF EXISTS t;
DROP TABLE t; -- {ErrorCode 25}

DROP TABLE system.null; -- {ErrorCode 2}
