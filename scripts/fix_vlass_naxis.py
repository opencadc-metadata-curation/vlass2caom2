#!/bin/bash

cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ams/cirada "SELECT COUNT(*) FROM caom2.Observation AS O JOIN caom2.Plane AS P ON O.obsID = P.obsID JOIN caom2.Artifact AS A ON P.planeID = A.planeID JOIN caom2.Part AS PR ON PR.artifactID = A.artifactID JOIN caom2.Chunk AS C ON C.partID = PR.partID WHERE O.collection='VLASS' AND C.naxis = 5" | grep -v "^$" | grep -v affected | grep -v observationID | grep -v "\-\-\-\-\-\-\-\-\-"
