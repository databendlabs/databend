SET enable_planner_v2=1;
SET max_threads=11;
SET unknown_settings=11; -- {ErrorCode 2801}
SHOW SETTINGS;
SHOW SETTINGS LIKE 'enable%';

-- This will messy the query results
-- SET GLOBAL max_threads=12;
-- SHOW SETTINGS LIKE 'max_threads';

SET enable_planner_v2=0;
