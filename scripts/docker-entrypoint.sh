#!/bin/bash

if [[ ! -e ${PWD}/config.yml ]]
then
  cp /usr/local/bin/config.yml ${PWD}
fi

if [[ ! -e ${PWD}/config.yml ]]
then
  cp /usr/local/bin/cache.yml ${PWD}
fi

if [[ ! -e ${PWD}/state.yml ]]
then
  yesterday=$(date -d yesterday "+%d-%b-%Y %H:%M")
  echo "bookmarks:
  vlass_timestamp:
    last_record: $yesterday
context:
  vlass_context:
    VLASS1.1: 01-Jan-2018 00:00
    VLASS1.2: 01-Nov-2018 00:00
    VLASS2.1: 01-Jul-2020 00:00
" > ${PWD}/state.yml
fi

exec "${@}"
