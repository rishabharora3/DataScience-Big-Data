SELECT prod.pid, COUNT(prod.pid) AS c
FROM person p JOIN producer prod
ON prod.pid = p.id 
JOIN movie m 
ON prod.mid = m.id 
JOIN moviegenre mg 
ON m.id = mg.mid 
JOIN genre g 
ON mg.gid = g.id
WHERE g.name = 'Action' 
AND prod.pid in 
(SELECT prod.pid
FROM producer prod JOIN director d 
ON prod.pid=d.pid)
AND prod.pid NOT IN 
(SELECT prod.pid
FROM producer prod JOIN person p 
ON prod.pid = p.id 
JOIN movie m 
ON prod.mid = m.id 
JOIN moviegenre mg 
ON m.id = mg.mid 
JOIN genre g 
ON mg.gid = g.id 
WHERE g.name = 'Romance')
GROUP BY prod.pid
HAVING c >= 15