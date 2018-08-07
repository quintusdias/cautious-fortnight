#!/bin/bash
#
# Run customized web log analytics of nowCOAST/IDP-GIS web logs.
#
# Arguments:
#   Path to gzipped apache log file.  The log file must be of the
#   form "project.ncep.noaa.gov_YYYYMM.gz".

set -e
set -x
logfile=${1}

# Tease the project name out of the log file.
project=$(basename $logfile | awk -F"." '{print $1}')

# Create a summary CSV file from the log.  That summary filename will
# be "hits.csv"
parse_logs.sh $logfile
join2html.py

# Move the results to local storage.
output_filename=$(basename $logfile | sed 's/.gz/.dat/g')
dest=$HOME/data/webstats/$project/$output_filename
mv hits.csv $dest

# Generate HTML and images from the historical CSV files.
update_monthly_content.py $project

# And transfer the content to the NCO intranet.
xfer_output.sh $project
