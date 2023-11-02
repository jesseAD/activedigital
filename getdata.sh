#!/bin/sh
source /home/ec2-user/.bash_profile
cd "/data/datacollector"
export MALLOC_ARENA_MAX=4
cat /dev/null > nohup.out
PRGCOU=`ps -eaf | grep -i data_getter | grep -v grep | wc -l`
if [ $PRGCOU -eq 0 ]
then
python3 /data/datacollector/src/data_getter.py
fi
sleep 5
exit
