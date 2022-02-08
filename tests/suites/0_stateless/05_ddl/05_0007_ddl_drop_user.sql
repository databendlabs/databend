DROP USER 'test-j'@'localhost'; -- {ErrorCode 2201}
DROP USER IF EXISTS 'test-j'@'localhost';


CREATE USER 'test-j'@'localhost' IDENTIFIED BY 'password';
DROP USER 'test-j'@'localhost';
DROP USER IF EXISTS 'test-j'@'localhost';

CREATE USER 'test-k'@'localhost' IDENTIFIED WITH plaintext_password BY 'password';
DROP USER IF EXISTS 'test-k'@'localhost';
DROP USER 'test-k'@'localhost'; -- {ErrorCode 2201}

CREATE USER 'test-l'@'localhost' IDENTIFIED WITH sha256_password BY 'password';
DROP USER 'test-l'@'localhost';
DROP USER IF EXISTS 'test-l'@'localhost';
DROP USER IF EXISTS 'test-l'@'localhost';
DROP USER IF EXISTS 'test-l'@'localhost';
