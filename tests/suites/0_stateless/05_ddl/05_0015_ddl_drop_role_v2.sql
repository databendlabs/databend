set enable_planner_v2 = 1;
DROP ROLE 'test-b'; -- {ErrorCode 2204}
DROP ROLE IF EXISTS 'test-b';


CREATE ROLE 'test-b';
DROP ROLE 'test-b';
DROP ROLE 'test-b'; -- {ErrorCode 2204}
DROP ROLE IF EXISTS 'test-b';
set enable_planner_v2 = 0;
