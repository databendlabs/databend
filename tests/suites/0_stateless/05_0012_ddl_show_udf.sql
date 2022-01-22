SHOW FUNCTION isnotempty; -- {ErrorCode 2602}

CREATE FUNCTION isnotempty AS (p) -> not(isnull(p)) DESC = 'This is a description';
SHOW FUNCTION isnotempty;
