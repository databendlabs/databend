SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user ORDER BY salary ASC;

SELECT arg_min(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);

set max_threads=8;
SELECT arg_min(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);

set max_threads=16;

SELECT arg_min(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);
