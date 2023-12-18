#!/bin/sh
source /home/ec2-user/.bash_profile
cd "/data/datacollector"
export MALLOC_ARENA_MAX=4
cat /dev/null > nohup.out
PRGCOU=`ps -eaf | grep -i data_getter1 | grep -v grep | wc -l`
if [ $PRGCOU -eq 0 ]
then
source /home/ec2-user/datacollector/bin/activate
python /data/datacollector/src/data_getter1.py
fi
sleep 5
exit
