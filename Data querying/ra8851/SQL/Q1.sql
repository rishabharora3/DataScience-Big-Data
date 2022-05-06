SELECT a.pid,COUNT(m.year)
FROM (Person AS p JOIN ACTOR AS a
ON p.id = a.pid)
JOIN Movie AS m
ON m.id = a.mid
WHERE p.dyear is NULL AND m.year = 2016
AND m.runtime > 90 AND a.pid NOT IN
(SELECT a1.pid
FROM ACTOR AS a1 JOIN Movie AS m1
ON a1.mid = m1.id
WHERE year >= 2017)
GROUP BY a.pid
HAVING COUNT(m.year) > 3
