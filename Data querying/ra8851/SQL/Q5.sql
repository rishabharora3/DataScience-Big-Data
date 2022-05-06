SELECT p.id, COUNT(p.id) as c
FROM Person as p JOIN actor as a
ON p.id = a.pid
JOIN Movie AS m 
ON m.id = a.mid
JOIN MovieGenre AS mg
ON m.id = mg.mid
JOIN Genre AS g
ON mg.gid = g.id
WHERE g.name = 'Drama'
and (p.name LIKE BINARY '%Patel' or p.name LIKE BINARY '%Raj')
and p.dyear is NULL and p.id NOT IN
(
SELECT p1.id
FROM Person as p1 JOIN actor as a1
ON p1.id = a1.pid JOIN
Movie AS m1 
ON m1.id = a1.mid
JOIN MovieGenre AS mg1
ON m1.id = mg1.mid
JOIN Genre AS g1
ON mg1.gid = g1.id
WHERE g1.name = 'Comedy'
)
GROUP BY(p.id)
HAVING c >= 5
