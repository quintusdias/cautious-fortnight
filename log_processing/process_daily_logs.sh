#!/usr/bin/env bash

set -x

for project in nowcoast idpgis
do

    # prune-arcgis-apache-database $project

    root=$HOME/data/logs/akamai/"$project"/incoming
    
    # Delete files that are older than a week.
    find "$root" -mtime +10080 | xargs -I fname rm fname
    
    # Get any new log files.
    get_akamai_logs $project
    
    # Process files just recently downloaded
    files_to_process=$(find "$root" -mmin -60 -name "*.gz" | sort -t "-" -k 3,3n -k4,4 -k5,5n)
    
    for logfile in $files_to_process
    do
    	parse-ag-ap-pg-logs $project --infile $logfile
    done

    produce-ag-ap-pg-graphics $project

    # Delete any files that are too old
    for datenum in $(seq 20 25)
    do
        datestr=$(date +%Y%m%d --date="-""$datenum"" days")
        rm $HOME/data/logs/akamai/$project/incoming/*.*.*-*-"$datestr"*.gz
    done	
    
done

rsync -avz ~/Documents/arcgis_apache_logs/*.{html,png} jevans@cerebrus:/var/www/html/gis

