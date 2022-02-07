CREATE FUNCTION isnotempty AS (p) -> not(isnull(p));
CREATE FUNCTION IF NOT EXISTS isnotempty AS (p) ->  not(isnull(p));
CREATE FUNCTION isnotempty AS (p) -> not(isnull(p)); -- {ErrorCode 2603}
CREATE FUNCTION isnotempty_with_desc AS (p) -> not(isnull(p)) DESC = 'This is a description';
CREATE FUNCTION IF NOT EXISTS isnotempty_with_desc AS (p) -> not(isnull(p)) DESC = 'This is a description';
CREATE FUNCTION isnotempty_with_desc AS (p) -> not(isnull(p)) DESC = 'This is a description'; -- {ErrorCode 2603}
