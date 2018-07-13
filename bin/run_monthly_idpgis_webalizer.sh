#!/bin/bash

set -x
logid=${1}

#webalizer -c $HOME/etc/webalizer/monthly/nowcoast.conf
#for logfile in $HOME/data/logs/idpgis/monthly/idpgis*.gz; do
#	echo $logfile
#	dest=$HOME/data/logs/idpgis/monthly/latest.gz
#	cp $logfile $dest
#	webalizer -c $HOME/etc/webalizer/monthly/idpgis.conf
#done
root=$HOME/etc/webalizer/monthly
sed "s/LATEST_LOG_ID/$logid/g" < $root/idpgis.conf.template > $root/idpgis.conf
webalizer -c $HOME/etc/webalizer/monthly/idpgis.conf

xfer_webalizer.sh
