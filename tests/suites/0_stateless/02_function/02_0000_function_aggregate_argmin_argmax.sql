SELECT sum(number) AS salary, number%3 AS user_name FROM numbers_mt(10000) GROUP BY user_name ORDER BY salary ASC;

SELECT arg_min(user_name, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user_name FROM numbers_mt(10000) GROUP BY user_name);

set max_threads=8;
SELECT arg_min(user_name, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user_name FROM numbers_mt(10000) GROUP BY user_name);

set max_threads=16;

SELECT arg_min(user_name, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user_name FROM numbers_mt(10000) GROUP BY user_name);
