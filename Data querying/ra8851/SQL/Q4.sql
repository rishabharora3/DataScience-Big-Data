SELECT p.id, COUNT(p.id) AS c,ROUND(AVG(m.rating),2) AS rating
FROM (director AS d JOIN Movie as m
ON d.mid = m.id)  
JOIN moviegenre as mg
ON mg.mid = m.id
JOIN genre as g
ON g.id = mg.gid
JOIN person as p
ON p.id = d.pid
WHERE g.name = 'Sci-fi' and m.totalvotes >= 1000
GROUP BY p.id
HAVING c >= 5
ORDER BY rating
LIMIT 15



