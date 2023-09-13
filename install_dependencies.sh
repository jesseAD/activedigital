#!/bin/sh
source /home/ec2-user/.bash_profile
cd "/data/datacollector"
export MALLOC_ARENA_MAX=4
cat /dev/null > nohup.out
pip3 install  --upgrade --force-reinstall -r /data/datacollector/requirements.txt
exit
