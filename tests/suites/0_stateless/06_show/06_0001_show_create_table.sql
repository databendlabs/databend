DROP DATABASE IF EXISTS `test`;
CREATE DATABASE `test`;
CREATE TABLE `test`.`a` (
    a bigint, b int default 3, c varchar(255) default 'x', d smallint null, e Date
) Engine = Null;
SHOW CREATE TABLE `test`.`a`;
CREATE TABLE `test`.`b` (
    a bigint, b int null default null, c varchar(255), d smallint unsigned null, e Date default today()
) Engine = Null COMMENT = 'test b';
SHOW CREATE TABLE `test`.`b`;

CREATE TABLE test.c (a int) CLUSTER BY (a, a % 3);
SHOW CREATE TABLE `test`.`c`;

-- SELECT '====SHOW CREATE TABLE WITH COLUMN COMMENT====';
-- set enable_planner_v2 = 1;
-- CREATE TABLE `test`.`d` (a INT COMMENT 'comment for a', b FLOAT NULL DEFAULT 0 COMMENT 'comment for b');
-- SHOW CREATE TABLE `test`.`d`;
-- set enable_planner_v2 = 0;


DROP TABLE `test`.`a`;
DROP TABLE `test`.`b`;
DROP TABLE `test`.`c`;
-- DROP TABLE `test`.`d`;
DROP DATABASE `test`;
