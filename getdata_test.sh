#!/bin/sh
source /home/ec2-user/.bash_profile
cd "/data/datacollector"
export MALLOC_ARENA_MAX=4
cat /dev/null > nohup.out
PRGCOU=`ps -eaf | grep -i test_dask | grep -v grep | wc -l`
if [ $PRGCOU -eq 0 ]
then
source /home/ec2-user/datacollector/bin/activate
python /data/datacollector/tests/test_dask.py
fi
sleep 5
exit
