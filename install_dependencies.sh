#!/bin/sh
source /home/ec2-user/.bash_profile
cd "/data/datacollector"
cp -R /data/datacollector/.config/dask/ /home/ec2-user/.config/dask
export MALLOC_ARENA_MAX=4
cat /dev/null > nohup.out
source /home/ec2-user/datacollector/bin/activate
pip install --upgrade --force-reinstall -r /data/datacollector/requirements.txt
exit
