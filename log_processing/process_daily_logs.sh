#!/usr/bin/env bash

set -x

for project in nowcoast idpgis
do

    agp-prune-database $project

    root=$HOME/data/logs/akamai/"$project"/incoming
    
    # Get any new log files.
    get_akamai_logs $project
    
    # Process files just recently downloaded
    files_to_process=$(find "$root" -mmin -60 -name "*.gz" | sort -t "-" -k 3,3n -k4,4 -k5,5n)
    
    for logfile in $files_to_process
    do
    	agp-parse-logs $project --infile $logfile
    done

    agp-produce-graphics $project

    # Delete files that are older than a week.
    #find "$root" -mtime +100080 | xargs -I fname rm fname
    
done

rsync -avz ~/Documents/arcgis_apache_logs/*.{html,png} jevans@cerebrus:/var/www/html/gis

