SELECT a.pid, COUNT(a.pid)
FROM (Person AS p
JOIN Actor as a
ON p.id = a.pid)
JOIN Movie AS m
ON m.id = a.mid
WHERE p.dyear is NULL AND m.year = 2021
AND m.adult = 1 AND a.pid NOT IN
(SELECT a1.pid
FROM ACTOR AS a1 JOIN Movie AS m1
ON a1.mid = m1.id
WHERE m1.year < 2021 AND m1.adult = 1)
GROUP BY a.pid
ORDER BY COUNT(a.pid) DESC, a.pid
LIMIT 25
