-- {ErrorCode 3, but it not work, because it's trimed in msql-srv}
USE not_exists_db; -- {ErrorCode 3}
USE default;
USE system;
select database();
