DROP TABLE IF EXISTS t;

CREATE TABLE t(c1 int) ENGINE = Null;

RENAME TABLE t TO t0;
DROP TABLE t; -- {ErrorCode 1025}
DROP TABLE t0;
DROP TABLE t0; -- {ErrorCode 1025}

DROP TABLE system.null; -- {ErrorCode 1002}
