#!/bin/bash
for i in `seq 1 1000`;
do
  echo $i
  echo "-----------------------------------------------------"
  go test $@
  if [ $? -ne 0 ] ; then
    exit
  fi
done
