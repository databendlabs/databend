SELECT REGEXP_INSTR('dog cat dog', 'dog');
SELECT REGEXP_INSTR('dog cat dog', 'dog', 2);
SELECT REGEXP_INSTR('dog cat dog', 'dog', 1, 2);
SELECT REGEXP_INSTR('dog cat dog', 'dog', 1, 2, 1);
SELECT REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, 1);
SELECT REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, 1, 'c');
SELECT REGEXP_INSTR('dog cat dog', NULL);
SELECT REGEXP_INSTR('dog cat dog', 'dog', NULL);
SELECT REGEXP_INSTR('🍣🍣b', 'b', 2);
SELECT REGEXP_INSTR('µå周çб', '周');
SELECT REGEXP_INSTR('周 周周 周周周 周周周周', '周+', 2, 3, 1);
--
DROP TABLE IF EXISTS t1;
CREATE TABLE t1(s String NULL, pat String NULL, pos Int64 NULL, occu Int64 NULL, ro Int64 NULL, mt String NULL) Engine = Memory;
INSERT INTO t1 (s, pat, pos, occu, ro, mt) VALUES (NULL, 'dog', 1, 1, 1, ''), ('dog cat dog', 'dog', NULL, 1, 1, 'c'), ('dog cat dog', 'dog', 1, 1, 1, 'c'), ('dog cat dog', 'dog', 1, 1, 1, NULL);
select s from t1 where regexp_instr(s, pat, pos, occu, ro, mt) = 4;
drop table t1;

