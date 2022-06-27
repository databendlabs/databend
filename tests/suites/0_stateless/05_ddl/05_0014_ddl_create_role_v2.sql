set enable_planner_v2 = 1;
CREATE ROLE 'test-a';
CREATE ROLE 'test-a'; -- {ErrorCode 2202}
CREATE ROLE IF NOT EXISTS 'test-a';
DROP ROLE 'test-a';
set enable_planner_v2 = 0;
