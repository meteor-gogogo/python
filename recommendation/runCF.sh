#!/bin/bash
VERSION=1.0.0

CURRDIR=`dirname "$0"`
EXECUTEDIR=`cd "$CURRDIR"; pwd`

if [ ! -d "$EXECUTEDIR"/logs ]; then
   mkdir "$EXECUTEDIR"/logs
fi

logName="$EXECUTEDIR"/logs/cf_`date +%Y%m%d`.log
touch "$logName"
source /home/aplum/.bashrc
source activate python36
#echo "$logName"

python main.py 7 >> "$logName"

exit 0

