#!/usr/bin/env bash

execNumber=0
errNumber=0
startTime=$(date)

function finish {
  echo
  echo '开始时间:' $startTime
  echo '结束时间:' $(date)
  echo '执行了:' $execNumber '次'
  echo '出现错误' $errNumber '次'
}

trap finish EXIT

for((i=1;;i++));
do
  result=$(go test -run 2B)
  strB="FAIL"

  if [[ $result =~ $strB ]]
  then
      echo "出现错误"
      echo $result >> error.text
      errNumber=$[$errNumber+1]
  fi
  execNumber=$[$execNumber+1]
  echo '执行了:' $execNumber '次'
done





