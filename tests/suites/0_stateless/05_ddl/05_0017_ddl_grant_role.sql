GRANT ROLE 'test' TO 'test-user'; -- {ErrorCode 2204}
CREATE ROLE 'test';
GRANT ROLE 'test' TO 'test-user'; -- {ErrorCode 2201}

CREATE USER 'test-user' IDENTIFIED BY 'password';
GRANT ROLE 'test' TO 'test-user';

GRANT ROLE 'test' TO ROLE 'test-role'; -- {ErrorCode 2204}
CREATE ROLE 'test-role';
GRANT ROLE 'test' TO ROLE 'test-role';

DROP ROLE 'test';
DROP ROLE 'test-role';
DROP USER 'test-user';
