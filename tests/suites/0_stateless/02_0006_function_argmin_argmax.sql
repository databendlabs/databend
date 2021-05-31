SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user ORDER BY salary ASC;

SELECT argMin(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);

set max_threads=8;
SELECT argMin(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);

set max_threads=16;

SELECT argMin(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);
