#!/bin/bash

if [[ ! -e config.yml ]]
then
    echo "No config.yml file. Stopping."
    date
    exit -1
fi

CONTAINER="bucket.canfar.net/vlass2caom2"
echo "Get the container"
docker pull ${CONTAINER} || exit $?

echo "Generate todo list"
./visitor_query.sh

echo "Run container ${CONTAINER}"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ ${CONTAINER} vlass_run || exit $?

date
exit 0

