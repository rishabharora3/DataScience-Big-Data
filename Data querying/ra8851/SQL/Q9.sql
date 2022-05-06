SELECT k.pid, COUNT(distinct k.mid) as c, round(AVG(m.rating),2) as ag
from person p JOIN knownfor k
ON p.id = k.pid JOIN movie m
ON k.mid = m.id 
where p.name LIKE binary 'Steve%'
and p.dyear is NULL
and k.mid IN
(SELECT k1.mid
from knownfor k1 JOIN movie m1 
ON k1.mid = m1.id JOIN moviegenre mg1 
ON m1.id = mg1.mid JOIN genre g1
ON mg1.gid = g1.id
WHERE g1.name = 'Drama' or g1.name = 'Thriller')
and k.mid NOT IN
(SELECT d.mid
FROM director d
GROUP BY d.mid
HAVING COUNT(d.pid) > 1)
GROUP BY k.pid
HAVING c >= 4
ORDER BY ag DESC, k.pid