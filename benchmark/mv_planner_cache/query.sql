SELECT
    a,
    b,
    min(t),
    max(t),
    sum(t),
    count(t),
    avg(t),
    approx_count_distinct(t)
FROM source
WHERE c = 'xxxxxxx'
GROUP BY a, b;
