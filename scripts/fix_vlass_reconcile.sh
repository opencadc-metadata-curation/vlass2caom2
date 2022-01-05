#!/bin/bash

CONTAINER="bucket.canfar.net/vlass2caom2"

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the container"
docker pull ${CONTAINER} || exit $?

echo "Generate reconcile todo.txt"
docker run --rm --name vlass_reconcile -v ${PWD}:/usr/src/app/ --user $(id -u):$(id -g) -e HOME=/usr/src/app ${CONTAINER} generate_reconciliation_todo || exit $?

echo "Reconcile"
docker run --rm --name vlass_reconcile -v ${PWD}:/usr/src/app/ --user $(id -u):$(id -g) -e HOME=/usr/src/app ${CONTAINER} vlass_run || exit $?

date
exit 0

