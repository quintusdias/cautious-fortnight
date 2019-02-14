#!/bin/bash

set -x

root=$HOME/Documents/daily_logs

# Produce a CSV of referer information from yesterday's logs
datestr=$(date +%Y%m%d -d "-1 days")

csvfile=$root/nowcoast/referer."$datestr".csv
zcat $HOME/data/logs/akamai/nowcoast/incoming/*.*.*-*-"$datestr"*.gz \
    | awk -f referer.awk \
    | tee $csvfile

python referer_to_db.py $csvfile $datestr
python process_referer_graphics.py

rm $csvfile
