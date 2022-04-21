CREATE FUNCTION isnotempty AS (p) -> not(is_null(p));
CREATE FUNCTION IF NOT EXISTS isnotempty AS (p) ->  not(is_null(p));
CREATE FUNCTION isnotempty AS (p) -> not(is_null(p)); -- {ErrorCode 2603}
CREATE FUNCTION isnotempty_with_desc AS (p) -> not(is_null(p)) DESC = 'This is a description';
CREATE FUNCTION IF NOT EXISTS isnotempty_with_desc AS (p) -> not(is_null(p)) DESC = 'This is a description';
CREATE FUNCTION isnotempty_with_desc AS (p) -> not(is_null(p)) DESC = 'This is a description'; -- {ErrorCode 2603}
