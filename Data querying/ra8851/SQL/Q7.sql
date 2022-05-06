SELECT w.pid
FROM writer w JOIN person p
ON w.pid = p.id
JOIN movie m
ON w.mid = m.id 
AND MATCH(m.otitle) AGAINST ('Christ')
AND MATCH(m.otitle) AGAINST('Jesus')
WHERE p.dyear IS NULL
AND m.rating > 
(SELECT MAX(rating)
FROM director d JOIN person p 
ON d.pid = p.id
JOIN movie m1
ON d.mid = m1.id 
WHERE p.name = 'Edward D. Wood Jr.')