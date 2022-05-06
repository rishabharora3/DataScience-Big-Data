SELECT m.id, m.otitle,m.runtime
FROM (Movie AS m JOIN MovieGenre AS mg
ON m.id = mg.mid)
JOIN Genre AS g
ON mg.gid = g.id
WHERE g.name = 'Comedy' AND year BETWEEN 1980 AND 1999
AND m.rating > 6.5 and m.totalvotes > 10000 AND
EXISTS
(
SELECT m1.id
FROM (Movie AS m1 JOIN MovieGenre AS mg1
ON m1.id = mg1.mid)
JOIN Genre AS g1
ON mg1.gid = g1.id
WHERE g1.name = 'Comedy'
AND m1.year > m.year AND m1.year<=1999 AND
m1.otitle LIKE CONCAT(m.otitle,'%')
)

