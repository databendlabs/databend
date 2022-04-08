DROP DATABASE IF EXISTS `test`;
CREATE DATABASE `test`;
CREATE TABLE `test`.`a` (
    a bigint, b int default 3, c varchar(255) default 'x', d smallint null, e Date
) Engine = Null;
SHOW CREATE TABLE `test`.`a`;
CREATE TABLE `test`.`b` (
    a bigint, b int null default null, c varchar(255), d smallint, e Date default today()
) Engine = Null COMMENT = 'test b';
SHOW CREATE TABLE `test`.`b`;
DROP TABLE `test`.`a`;
DROP TABLE `test`.`b`;
DROP DATABASE `test`;
