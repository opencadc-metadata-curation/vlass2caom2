#!/bin/bash

if [[ ! -e ${PWD}/config.yml ]]
then
  cp /usr/local/.config/config.yml ${PWD}
fi

if [[ ! -e ${PWD}/state.yml ]]
then
  yesterday=$(date -d yesterday "+%Y-%m-%d %H:%M:%S")
  echo "bookmarks:
  https://archive-new.nrao.edu/vlass/quicklook/:
    last_record: $yesterday
  https://archive-new.nrao.edu/vlass/se_continuum_imaging/:
    last_record: $yesterday
context:
  vlass_context:
    VLASS1.1v2: 2018-01-01 00:00
    VLASS1.2v2: 2018-11-01 00:00
    VLASS2.1: 2020-07-01 00:00
    VLASS2.2: 2021-09-01 00:00
    VLASS3.1: 2022-06-01 00:00
    VLASS3.2: 2024-04-01 00:00
" > ${PWD}/state.yml
fi

exec "${@}"
