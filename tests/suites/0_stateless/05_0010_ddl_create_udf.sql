CREATE FUNCTION isnotnull='not(isnull(@0))';
CREATE FUNCTION IF NOT EXISTS isnotnull='not(isnull(@0))';
CREATE FUNCTION isnotnull='not(isnull(@0))'; -- {ErrorCode 4072}
CREATE FUNCTION isnotempty='not(isempty(@0))' desc='This is a description';
CREATE FUNCTION IF NOT EXISTS isnotempty='not(isempty(@0))' desc='This is a description';
CREATE FUNCTION isnotempty='not(isempty(@0))' desc='This is a description'; -- {ErrorCode 4072}
