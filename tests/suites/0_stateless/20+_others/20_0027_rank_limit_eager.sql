SELECT
    b.number AS bn,
    SUM(a.number) AS s
FROM
    numbers(100) AS a
    JOIN numbers(100) AS b ON a.number = b.number
GROUP BY
    b.number
ORDER BY
    bn
LIMIT
    5;
