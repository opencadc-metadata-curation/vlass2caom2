#!/bin/bash

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the container"
docker pull bucket.canfar.net/vlass2caom2 || exit $?

echo "Run vlass_run container"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ bucket.canfar.net/vlass2caom2 vlass_run_state || exit $?

date
exit 0
