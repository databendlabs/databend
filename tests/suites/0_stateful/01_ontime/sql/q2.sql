SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2019 AND Year<=2021
GROUP BY DayOfWeek
ORDER BY c DESC;