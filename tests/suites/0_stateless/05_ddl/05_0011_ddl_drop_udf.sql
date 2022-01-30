DROP FUNCTION IF EXISTS isnotempty;

CREATE FUNCTION isnotempty AS (p) -> not(isnull(p));
DROP FUNCTION isnotempty;

DROP FUNCTION IF EXISTS isnotempty;

DROP FUNCTION isnotempty; -- {ErrorCode 2602}
