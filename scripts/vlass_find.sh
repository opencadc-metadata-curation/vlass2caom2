#!/bin/bash

query="
SELECT A.uri
FROM caom2.Observation as O
JOIN caom2.Plane as P on O.obsID = P.obsID
JOIN caom2.Artifact as A on P.planeID = A.planeID
WHERE O.collection = 'VLASS'
AND P.planeID IN (
  SELECT A.planeID
  FROM caom2.Observation as O
  JOIN caom2.Plane as P on O.obsID = P.obsID
  JOIN caom2.Artifact as A on P.planeID = A.planeID
  WHERE collection = 'VLASS'
  GROUP BY A.planeID
  HAVING COUNT(A.artifactID) > 2
)
"
cadc-tap query --debug --netrc ./netrc -s ivo://cadc.nrc.ca/ams/cirada "${query}" | grep -v count | grep -v "\-\-\-\-\-\-\-\-" | grep -v "^$" | grep -v affected
exit 0
