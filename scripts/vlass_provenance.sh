#!/bin/bash

# cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ams/cirada "SELECT COUNT(*) FROM caom2.Observation AS O JOIN caom2.Plane AS P ON O.obsID = P.obsID JOIN caom2.Artifact AS A ON P.planeID = A.planeID JOIN caom2.Part AS PR ON PR.artifactID = A.artifactID JOIN caom2.Chunk AS C ON C.partID = PR.partID WHERE O.collection='VLASS' AND C.naxis = 5" | grep -v "^$" | grep -v affected | grep -v observationID | grep -v "\-\-\-\-\-\-\-\-\-"
cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ams/cirada "
SELECT A.uri
FROM caom2.Artifact AS A
WHERE A.uri like 'ad:VLASS/VLASS1.2%fits'
"

# cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ad "
# SELECT fileName from archive_files
# WHERE archiveName = 'GEM'
# AND fileName LIKE '%jpg'
# AND ingestDate >= '2018-11-01 12:10:10'
# AND ingestDate < '2018-11-04 12:10:10'
# ORDER BY ingestDate ASC
# "

# cadc-tap query --debug --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ams/gemini "
# SELECT A.planeID, COUNT(DISTINCT A.uri)
# FROM caom2.Artifact AS A
# WHERE A.uri like 'gemini:GEM%fits'
# GROUP BY A.planeID
# HAVING COUNT(A.artifactID) = 1
# LIMIT 1"

# | grep -v "^$" | grep -v affected | grep -v observationID | grep -v "\-\-\-\-\-\-\-\-\-"
