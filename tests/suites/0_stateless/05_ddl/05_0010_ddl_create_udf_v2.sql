set enable_planner_v2=1;

DROP FUNCTION IF EXISTS isnotempty;

CREATE FUNCTION isnotempty AS (p) -> not(is_null(p));
CREATE FUNCTION IF NOT EXISTS isnotempty AS (p) ->  not(is_null(p));
CREATE FUNCTION isnotempty AS (p) -> not(is_null(p)); -- {ErrorCode 2603}

DROP FUNCTION IF EXISTS isnotempty_with_desc;

CREATE FUNCTION isnotempty_with_desc AS (p) -> not(is_null(p)) DESC = 'This is a description';
CREATE FUNCTION IF NOT EXISTS isnotempty_with_desc AS (p) -> not(is_null(p)) DESC = 'This is a description';
CREATE FUNCTION isnotempty_with_desc AS (p) -> not(is_null(p)) DESC = 'This is a description'; -- {ErrorCode 2603}
