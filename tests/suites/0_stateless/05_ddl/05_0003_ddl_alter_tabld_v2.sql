set enable_planner_v2 = 1;

-- Alter table rename
DROP TABLE IF EXISTS `05_0003_at_t0`;
DROP TABLE IF EXISTS `05_0003_at_t1`;

CREATE TABLE `05_0003_at_t0`(a int);
INSERT INTO TABLE `05_0003_at_t0` values(1);
SELECT * FROM `05_0003_at_t0`;

ALTER TABLE `05_0003_at_t0` RENAME TO `05_0003_at_t1`;
ALTER TABLE `05_0003_at_t0` RENAME TO `05_0003_at_t1`; -- {ErrorCode 1025}
ALTER TABLE IF EXISTS `05_0003_at_t0` RENAME TO `05_0003_at_t1`;
DROP TABLE `05_0003_at_t0`; -- {ErrorCode 1025}
SELECT * FROM `05_0003_at_t1`;

DROP TABLE IF EXISTS `05_0003_at_t1`;

set enable_planner_v2 = 0;
