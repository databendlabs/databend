DROP DATABASE IF EXISTS `test`;
CREATE DATABASE `test`;
CREATE TABLE `test`.`a` (
    a bigint, b int default 3, c varchar default 'x', d smallint null, e Date
) Engine = Null;
SHOW CREATE TABLE `test`.`a`;
CREATE TABLE `test`.`b` (
    a bigint, b int null default null, c varchar, d smallint unsigned null, e Date default today()
) Engine = Null COMMENT = 'test b';
SHOW CREATE TABLE `test`.`b`;

CREATE TABLE test.c (a int) CLUSTER BY (a, a % 3);
SHOW CREATE TABLE `test`.`c`;

DROP TABLE `test`.`a`;
DROP TABLE `test`.`b`;
DROP TABLE `test`.`c`;
DROP DATABASE `test`;
