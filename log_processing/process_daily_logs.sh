#!/usr/bin/env bash

set -x

# for project in nowcoast idpgis
for project in nowcoast idpgis
do

    prune-arcgis-apache-database $project

    root=$HOME/data/logs/akamai/"$project"/incoming
    
    # Delete files that are older than a week.
    find "$root" -mtime +10080 | xargs -I fname rm fname
    
    # Get any new log files.
    get_akamai_logs $project
    
    # Process files just recently downloaded
    files_to_process=$(find "$root" -mmin -60 -name "*.gz" | sort -t "-" -k 3,3n -k4,4 -k5,5n)
    
    for file in $files_to_process
    do
    	zcat $file | parse-arcgis-apache-logs $project --infile -
    done

    produce-arcgis-apache-graphics $project

    # Delete any files that are too old
    for datenum in $(seq 10 15)
    do
        datestr=$(date +%Y%m%d --date="-""$datenum"" days")
        rm $HOME/data/logs/akamai/$project/incoming/*.*.*-*-"$datestr"*.gz
    done	
    
done
