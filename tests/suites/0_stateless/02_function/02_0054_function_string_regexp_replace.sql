SELECT REGEXP_REPLACE('a b c', '( ){1,}', '');
SELECT REGEXP_REPLACE('a b c', '( ){1,}', '', 1, 1);
SELECT REGEXP_REPLACE('a b c', 'x', '', 1, 1);
SELECT REGEXP_REPLACE('a b c', '( ){1,}', '', 6, 1);
SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3);
SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 4);
SELECT REGEXP_REPLACE('abc def GHI', '[a-z]+', 'X', 1, 3, 'i');
SELECT REGEXP_REPLACE('🍣🍣b', 'b', 'X');
SELECT REGEXP_REPLACE('µå周çб周周', '周+', '唐', 1, 2);
SELECT REGEXP_REPLACE(NULL, 'b', 'X');
SELECT REGEXP_REPLACE('a b c', NULL, 'X');
SELECT REGEXP_REPLACE('a b c', 'b', NULL);
SELECT REGEXP_REPLACE('a b c', 'b', 'X', NULL);
SELECT REGEXP_REPLACE('a b c', 'b', 'X', 1, NULL);
SELECT REGEXP_REPLACE('a b c', 'b', 'X', 1, 2, NULL);
SELECT '======';
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(s String NULL, pat String NULL, repl String NULL, pos Int64 NULL, occu Int64 NULL, mt String NULL) Engine = Fuse;
INSERT INTO t1 (s, pat, repl, pos, occu, mt) VALUES (NULL, 'dog', '[a-z]+', 1, 1, ''), ('abc def ghi', NULL, 'X', 1, 1, 'c'), ('abc def ghi', '[a-z]+', NULL, 1, 1, 'c'), ('abc def ghi', '[a-z]+', 'X', NULL, 1, 'c'), ('abc def ghi', '[a-z]+', 'X', 1, NULL, 'c'), ('abc def ghi', '[a-z]+', 'X', 1, 1, NULL), ('abc def ghi', '[a-z]+', 'X', 1, 1, 'c');
SELECT s FROM t1 WHERE REGEXP_REPLACE(s, pat, repl, pos, occu, mt) = 'X def ghi';
DROP TABLE t1;
SELECT '======';
SELECT REGEXP_REPLACE('a b c', 'b', 'X', 0); -- {ErrorCode 1006}
SELECT REGEXP_REPLACE('a b c', 'b', 'X', 1, -1); -- {ErrorCode 1006}
SELECT REGEXP_REPLACE('a b c', 'b', 'X', 1, 0, '-i'); -- {ErrorCode 1006}
