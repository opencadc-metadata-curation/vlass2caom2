#!/bin/bash

COLLECTION="vlass"
IMAGE="bucket.canfar.net/${COLLECTION}2caom2"

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the image ${IMAGE}"
docker pull ${IMAGE}

echo "Run ${COLLECTION}_run"
docker run --rm --name ${COLLECTION}_validate -v ${PWD}:/usr/src/app/ ${IMAGE} ${COLLECTION}_validate || exit $?

date
exit 0
