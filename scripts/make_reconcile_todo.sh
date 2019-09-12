#!/bin/bash

rm todo.txt
for ii in $(cat validate_state.yml | awk '/at_nrao/,0' | grep -v at_nrao | awk '{print $2}' ); do
  grep $ii nrao_state.csv | awk -F\, '{print $2}' | awk '{print $1}' >> todo.txt
done
exit 0
