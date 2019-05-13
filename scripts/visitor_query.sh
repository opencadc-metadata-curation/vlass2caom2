#!/bin/bash

cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ams/cirada "SELECT O.observationID FROM caom2.Observation AS O JOIN caom2.Plane AS P ON O.obsID = P.obsID WHERE O.collection='VLASS' AND P.provenance_reference is NULL" | grep -v "^$" | grep -v affected | grep -v observationID | grep -v "\-\-\-\-\-\-\-\-\-" >& todo.txt

date
exit 0
