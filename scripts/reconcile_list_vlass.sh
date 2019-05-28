#!/bin/bash

echo "Get a certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get the container"
# docker pull bucket.canfar.net/vlass2caom2 || exit $?

echo "Run vlass_run container"
# docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ bucket.canfar.net/vlass2caom2 python vlass_validator.py || exit $?
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ vlass_val_test python /usr/local/lib/python3.6/site-packages/vlass2caom2/vlass_validator.py || exit $?

date
exit 0
