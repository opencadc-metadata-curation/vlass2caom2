#!/usr/bin/env bash

if [[ ! -e config.yml ]]
then
    echo "No config.yml file. Stopping."
    date
    exit -1
fi

echo "Generate todo list"
. ./visitor_query.sh

echo "Run vlass_run container"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ vlass_run vlass_run || exit $?

date
exit 0

