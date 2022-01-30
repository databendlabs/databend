SHOW CREATE DATABASE `system`;
DROP DATABASE IF EXISTS `test`;
CREATE DATABASE `test`;
SHOW CREATE DATABASE `test`;
DROP DATABASE `test`;
CREATE DATABASE `datafuselabs` ENGINE=github(token='xxx'); -- {ErrorCode 1073}
SHOW CREATE DATABASE `datafuselabs`;
DROP DATABASE `datafuselabs`;
