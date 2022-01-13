-- {ErrorCode 1003, but it not work, because it's trimed in msql-srv}
USE not_exists_db;
USE default;
USE system;
select database();
create database `rust-lang`;
use `rust-lang`;
