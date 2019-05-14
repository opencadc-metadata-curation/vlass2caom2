#!/bin/bash

if [[ ! -e config.yml ]]
then
    echo "No config.yml file. Stopping."
    date
    exit -1
fi

echo "Get the container"
docker pull bucket.canfar.net/vlass2caom2 || exit $?

echo "Generate todo list"
./visitor_query.sh

echo "Run vlass_run container"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ bucket.canfar.net/vlass2caom2 vlass_run || exit $?

date
exit 0

