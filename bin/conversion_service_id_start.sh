#!/bin/sh

WORK_HOME="/home/manager/svc/index/conversion_service_id"

echo $WORK_HOME

cd $WORK_HOME

echo "CONVERSION SERVICE ID START"

python $WORK_HOME/src/conversion_service_id.py

echo "CONVERSION SERVICE ID END"
