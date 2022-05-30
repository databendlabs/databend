SELECT DISTINCT * FROM numbers(3) order by number;
SELECT DISTINCT 1 FROM numbers(3);
SELECT DISTINCT (number %3) as c FROM numbers(1000) ORDER BY c;
