SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE Year>=2019 AND Year<=2021
GROUP BY DayOfWeek
ORDER BY c DESC;