#!/bin/bash

set -x

logid=${1}

# for logfile in $HOME/data/logs/nowcoast/monthly/nowcoast.ncep.noaa.gov_??????.gz; do
# 	echo $logfile
# 	dest=$HOME/data/logs/nowcoast/monthly/latest.gz
# 	cp $logfile $dest
# 	webalizer -c $HOME/etc/webalizer/monthly/nowcoast.conf
# done
root=$HOME/etc/webalizer/monthly
sed "s/LATEST_LOG_FILE/$logid/g" < $root/nowcoast.conf.template > $root/nowcoast.conf
webalizer -c $HOME/etc/webalizer/monthly/nowcoast.conf


