#!/usr/bin/env bash

set -x

# Get any new log files.
get_akamai_logs nowcoast

# Process logs from this many days ago, passed in as a command line argument.
num_days_back=${1:-"1"}

root=$HOME/Documents/daily_logs

# Delete any files that are too old
datestr=$(date +%Y%m%d --date="-7 days")
rm $HOME/data/logs/akamai/nowcoast/incoming/*.*.*-*-"$datestr"*.gz

# Produce a CSV of referer information from yesterday's logs
datestr=$(date +%Y%m%d --date="-$num_days_back days")
csvfile=$root/nowcoast/referer."$datestr".csv
mkdir -p $(dirname "$csvfile")
rm $csvfile

precision="hours"
gzip -dc $HOME/data/logs/akamai/nowcoast/incoming/*.*.*-*-"$datestr"*.gz \
    | PRECISION="$precision" awk -f most_referer_bandwidth.awk \
    | tee $csvfile

python referer_to_db.py $csvfile
python process_referer_graphics.py

rm $csvfile
