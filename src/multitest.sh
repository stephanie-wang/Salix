#!/bin/bash
for i in `seq 1 1000`;
do
  echo -e "\ec\e[3J"	# cls
  echo $i
  echo "-----------------------------------------------------"
  go test $@ 2>&1 | grep -v unexpected | grep -v connection | grep -v write
  if [ $? -ne 0 ] ; then
    echo "failed on $i"
    exit 1
  fi
done
