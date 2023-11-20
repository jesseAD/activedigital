#!/bin/sh
source /home/ec2-user/.bash_profile
cd "/data/datacollector"
export MALLOC_ARENA_MAX=4
cat /dev/null > nohup.out
source /home/ec2-user/datacollector/bin/activate
/usr/bin/python3.11 -m pip install --upgrade --force-reinstall -r /data/datacollector/requirements.txt
exit
