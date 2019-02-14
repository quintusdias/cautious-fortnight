#!/bin/bash

set -x

root=/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common/nowcoast/analytics/daily
root=/export/nco-lw-jevans2/jevans/Documents/daily_logs

export PATH=$HOME/git/gis-monitoring/bin:$PATH

# Produce a CSV of referer information from yesterday's logs
datestr=$(date +%Y%m%d -d "-1 days")

h5file=$root/nowcoast/rate-limiting."$datestr".h5
#    | egrep "(216.38.80.221|140.90.75.204)" \
zcat $HOME/data/logs/akamai/nowcoast/incoming/*.*.*-*-"$datestr"*.gz \
    | akamai_thresholds.py $h5file

