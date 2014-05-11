#!/bin/bash
logfile=$1
shift

for i in `seq 1 1000`;
do
  echo -e "\ec\e[3J"	# cls
  echo "logfile: $logfile"
  echo $i
  echo "-----------------------------------------------------"
  go test $@ 2>&1 | grep -v unexpected | grep -v broken | grep -v connection | grep -v method | tee $logfile
  if [ ${PIPESTATUS[0]} -ne 0 ] ; then
    echo "failed on $i"
    exit 1
  fi
done
