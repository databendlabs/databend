-- TENANT1
SUDO USE TENANT 'tenant1';

-- user check.
CREATE USER 'test'@'localhost' IDENTIFIED BY 'password';
ALTER USER 'test'@'localhost' IDENTIFIED BY 'password1';
SHOW USERS;

-- TENANT2
SUDO USE TENANT 'tenant2';
SHOW USERS;

-- TENANT1
SUDO USE TENANT 'tenant1';
DROP USER 'test'@'localhost';
