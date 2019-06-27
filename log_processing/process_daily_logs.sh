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
    files_to_process=$(find "$root" -mmin -60 -name "*.gz")
    
    # for filename in $files_to_process
    # do
    #     echo processing $filename
    #     gzip -dc "$filename" \
    #         | tee \
    #             >(python process_ips.py $project -) \
    #             >(python process_referer.py $project -) \
    #             >(python process_services.py $project -) \
    #     	1> /dev/null
    # done
    gzip -dc $files_to_process \
        | tee \
            >(python process_ips.py $project -) \
            >(python process_referer.py $project -) \
            >(python process_services.py $project -) \
    	1> /dev/null

    # Delete any files that are too old
    datestr=$(date +%Y%m%d --date="-10 days")
    rm $HOME/data/logs/akamai/$project/incoming/*.*.*-*-"$datestr"*.gz
    
done
