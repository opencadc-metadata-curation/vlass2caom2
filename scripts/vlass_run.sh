#!/bin/bash

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

IMAGE="bucket.canfar.net/vlass2caom2"
echo "Get the image ${IMAGE}"
docker pull ${IMAGE}

echo "Run ${IMAGE}"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ ${IMAGE} vlass_run || exit $?

date
exit 0
