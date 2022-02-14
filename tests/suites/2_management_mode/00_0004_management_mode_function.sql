-- TENANT1
SUDO USE TENANT 'tenant1';

-- udf check.
CREATE FUNCTION xy AS (x, y) -> (x + y) / 2;
ALTER FUNCTION xy AS (x, y) -> (x + y) / 3;
SHOW FUNCTIONS LIKE 'xy';


-- TENANT2
SUDO USE TENANT 'tenant2';
SHOW FUNCTIONS LIKE 'xy';

-- TENANT1
SUDO USE TENANT 'tenant1';
DROP FUNCTION xy;
