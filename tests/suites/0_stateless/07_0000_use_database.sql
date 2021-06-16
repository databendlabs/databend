USE not_exists_db; -- {ErrorCode 3}
USE default;
USE system;
select database();
