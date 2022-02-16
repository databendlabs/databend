SELECT CONCAT('My', 'S', 'QL');
SELECT CONCAT('My', NULL, 'QL');
SELECT CONCAT('14.3');
SELECT CONCAT('14.3', 'SQL');
select CONCAT(toString(number), 'a', toString(number+1)) from numbers(3) order by number;
SELECT CONCAT(toString(number), NULL) from numbers(4);
