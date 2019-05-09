#!/usr/bin/env bash

if [[ ! -e config.yml ]]
then
    echo "No config.yml file. Stopping."
    date
    exit -1
fi
echo "Get the container"
docker pull bucket.canfar.net/vlass2caom2 || exit $?

echo "Generate todo list"
cadc-tap query --cert $HOME/.ssl/cadcproxy.pem -s ivo://cadc.nrc.ca/ams/cirada "SELECT A.uri FROM caom2.Observation AS O JOIN caom2.Plane AS P ON O.obsID = P.obsID JOIN caom2.Artifact AS A ON P.planeID = A.planeID WHERE O.collection='VLASS' AND P.provenance_reference is NULL" | awk -F\/ '{print $2}' >& todo.txt || exit $?

echo "Run vlass_run container"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ vlass2caom2 vlass_run || exit $?

date
exit 0

