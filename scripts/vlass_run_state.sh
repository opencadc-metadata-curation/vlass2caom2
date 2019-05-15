#!/bin/bash

if [[ ! -e state.yml ]]
then
    echo "bookmarks:
  vlass_timestamp:
    last_record: 02-May-2019 10:31
" > state.yml
fi

echo "Get the container"
docker pull bucket.canfar.net/vlass2caom2 || exit $?

echo "Run vlass_run container"
docker run --rm --name vlass_run -v ${PWD}:/usr/src/app/ bucket.canfar.net/vlass2caom2 vlass_run_state || exit $?

date
exit 0
