SHOW FUNCTION isnotnull; -- {ErrorCode 4071}

CREATE FUNCTION isnotnull='not(isnull(@0))' desc='This is a description';
SHOW FUNCTION isnotnull;
