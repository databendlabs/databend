CREATE FUNCTION isnotempty=not(isnull(@0));
CREATE FUNCTION IF NOT EXISTS isnotempty=not(isnull(@0));
CREATE FUNCTION isnotempty=not(isnull(@0)); -- {ErrorCode 4072}
CREATE FUNCTION isnotempty_with_desc=not(isnull(@0)) DESC = 'This is a description';
CREATE FUNCTION IF NOT EXISTS isnotempty_with_desc=not(isnull(@0)) DESC = 'This is a description';
CREATE FUNCTION isnotempty_with_desc=not(isnull(@0)) DESC = 'This is a description'; -- {ErrorCode 4072}
