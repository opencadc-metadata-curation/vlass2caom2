#!/bin/bash

ARG_LEN=$(($#-1))

# Hack to get last argument.
for CERTIFICATE_STRING; do true; done

# Everything before certificate is an input.
INPUTS=${@:1:$ARG_LEN}

for INPUT_FILE in "${INPUTS[@]}"; do
  if [[ ! -z "${INPUT_FILE}" ]];
  then
    echo "Processing ${INPUT_FILE}."
    vlass_run_single "${INPUT_FILE}" "${CERTIFICATE_STRING}"
  fi
done
