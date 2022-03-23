-- TENANT1
SUDO USE TENANT 'tenant1';

SHOW USERS;
SHOW ROLES;

-- CALL admin$bootstrap_tenant
CALL admin$bootstrap_tenant('tenant1', 'test_user', '%', 'double_sha1_password', 'test123');

SHOW USERS;
SHOW ROLES;

DROP USER 'test_user';
DROP ROLE 'account_admin';
