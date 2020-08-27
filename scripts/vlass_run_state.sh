#!/bin/bash

CONTAINER="bucket.canfar.net/vlass2caom2"

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the container"
docker pull ${CONTAINER}

echo "Run container ${CONTAINER}“"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ ${CONTAINER} vlass_run_state || exit $?

date
exit 0
