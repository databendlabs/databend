SET enable_planner_v2=1;
DROP DATABASE IF EXISTS a;
DROP DATABASE IF EXISTS b;
CREATE DATABASE a;
CREATE TABLE a.t(id INT, id2 INT);
INSERT INTO a.t VALUES (1, 1),(2, 2);

SELECT id FROM a.t ORDER BY id2 ASC;
SELECT id FROM a.t ORDER BY t.id2 ASC;
SELECT id FROM a.t ORDER BY a.t.id2 ASC;

SELECT DISTINCT(id) FROM a.t ORDER BY a.t.id;
SELECT DISTINCT(id) FROM a.t ORDER BY a.t.id2; -- {ErrorCode 1065}
-- this will err but if has a alias will not err.
-- SELECT SUM(id) FROM a.t ORDER BY a.t.id2;
-- ERROR 1105 (HY000): Code: 1006, displayText = Unable to get field named ""id2"_1". Valid fields: ["\"SUM(id)\"_4"].
SELECT SUM(id) as id2 FROM a.t ORDER BY a.t.id2;
-- expect err
SELECT DISTINCT(id) as id2 FROM a.t ORDER BY a.t.id2;

SELECT * FROM a.t ORDER BY a.t.id ASC;
SELECT * FROM a.t ORDER BY B.T.id ASC;  -- {ErrorCode 1003}
SELECT * FROM a.t ORDER BY a.t.id DESC;
SELECT * FROM a.t ORDER BY B.T.id DESC; -- {ErrorCode 1003}
DROP DATABASE a;
SET enable_planner_v2=0;
