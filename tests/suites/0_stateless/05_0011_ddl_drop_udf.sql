DROP FUNCTION IF EXISTS isnotnull;

CREATE FUNCTION isnotnull='not(isnull(@0))';
DROP FUNCTION isnotnull;

DROP FUNCTION IF EXISTS isnotnull;

DROP FUNCTION isnotnull; -- {ErrorCode 4071}
