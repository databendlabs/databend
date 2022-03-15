REVOKE ROLE 'test' FROM 'test-user'; -- {ErrorCode 2201}
CREATE USER 'test-user' IDENTIFIED BY 'password';
REVOKE ROLE 'test' FROM 'test-user';

REVOKE ROLE 'test' FROM ROLE 'test-role'; -- {ErrorCode 2204}
CREATE ROLE 'test-role';
REVOKE ROLE 'test' FROM ROLE 'test-role';

DROP ROLE 'test-role';
DROP USER 'test-user';
