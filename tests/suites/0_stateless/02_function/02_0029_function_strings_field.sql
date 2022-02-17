-- 2 2 2 2 2
SELECT '===1===';
SELECT FIELD('3', '77', '3') FROM numbers(5) ORDER BY number;
-- 0 0 2 0 0
SELECT '===2===';
SELECT FIELD('3', '77', toString(number+1)) FROM numbers(5) ORDER BY number;
-- 0 0 2 0 0
SELECT '===3===';
SELECT FIELD(toString(number+1), '77', '3') FROM numbers(5) ORDER BY number;
-- 0 0 2 0 0
SELECT '===4===';
SELECT FIELD(toString(number), '77', toString(4-number)) FROM numbers(5) ORDER BY number;
