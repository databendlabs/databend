CREATE FUNCTION isnotempty='not(isnull(@0))';
CREATE FUNCTION IF NOT EXISTS isnotempty='not(isnull(@0))';
CREATE FUNCTION isnotempty='not(isnull(@0))'; -- {ErrorCode 4072}
