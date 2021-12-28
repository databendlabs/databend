SHOW FUNCTION isnotempty; -- {ErrorCode 4071}

CREATE FUNCTION isnotempty AS (p) -> not(isnull(p)) DESC AS 'This is a description';
SHOW FUNCTION isnotempty;
