#!/bin/bash

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the container"
# docker pull bucket.canfar.net/vlass2caom2 || exit $?
CONTAINER=vlass_run_int

echo "Run container ${CONTAINER}"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ ${CONTAINER} vlass_run || exit $?

date
exit 0
