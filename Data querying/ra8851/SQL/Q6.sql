SELECT DISTINCT k.pid
FROM KnownFor AS k
WHERE k.pid IN
(SELECT k1.pid 
FROM Person as p1 JOIN director AS d
ON p1.id = d.pid
JOIN KnownFor as k1
ON k1.mid = d.mid
WHERE p1.name = 'Sofia Coppola')
and k.pid IN
(
SELECT k2.pid 
FROM Person as p2 JOIN Actor AS a
ON p2.id = a.pid
JOIN KnownFor as k2
ON k2.mid = a.mid
WHERE p2.name = 'Antonio Banderas'
)
