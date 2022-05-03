SELECT CONCAT('My', 'S', 'QL');
SELECT CONCAT('My', NULL, 'QL');
SELECT CONCAT('14.3');
SELECT CONCAT('14.3', 'SQL');
select CONCAT(to_varchar(number), 'a', to_varchar(number+1)) from numbers(3) order by number;
SELECT CONCAT(to_varchar(number), NULL) from numbers(4);
