-- {ErrorCode 1003, but it not work, because it's trimed in opensrv-mysql}
USE not_exists_db;
USE ``;
USE default_db;
USE system;
select database();
create database `rust-lang`;
use `rust-lang`;

DROP DATABASE if exists `rust-lang`;
