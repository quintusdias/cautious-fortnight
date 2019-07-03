#!/usr/bin/env bash

set -x

for project in nowcoast idpgis
do

    root=$HOME/data/logs/akamai/"$project"/incoming
    
    # Delete files that are older than a week.
    find "$root" -mtime +10080 | xargs -I fname rm fname
    
    # Get any new log files.
    get_akamai_logs $project
    
    # Process files just recently downloaded
    files_to_process=$(find "$root" -mmin -60 -name "*.gz" | sort)
    
    zcat $files_to_process | parse-arcgis-apache-logs $project --infile -

    # Delete any files that are too old
    datestr=$(date +%Y%m%d --date="-10 days")
    rm $HOME/data/logs/akamai/$project/incoming/*.*.*-*-"$datestr"*.gz
    
done
