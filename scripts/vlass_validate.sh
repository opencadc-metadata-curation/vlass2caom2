#!/bin/bash

COLLECTION="vlass"
IMAGE="bucket.canfar.net/${collection}2caom2"

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the image ${IMAGE}"
docker pull ${IMAGE} || exit $?

echo "Run ${collection}_run"
docker run --rm --name ${collection}_validate -v ${PWD}:/usr/src/app/ ${IMAGE} ${collection}_validate || exit $?

date
exit 0
