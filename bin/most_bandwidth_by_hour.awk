#!/bin/bash
#
# Determine who requested the most bandwidth by the minute.
#
# Example usage:
# 
#   zcat apache_log.gz | awk -f most_bandwidth_by_minute.awk | sort -k1,1
#
# This may produce output that looks as follows:
#
#             ate           IP Address         MB    
#  09/Jul/2018:00       66.223.179.138      13.66
#  09/Jul/2018:01       66.223.179.138      14.13
#  09/Jul/2018:02        216.38.80.221      15.29
#
#

BEGIN {

    header_pspec = "%20s%45s%10s%10s\n"
    body_pspec = "%20s%45s%10.2f%10s\n"

    # Header line
    printf(header_pspec, "Date", "IP Address", "MB", "Hits")
}

{
    # We are interested in three fields here.  The 1st field has the
    # IP address.  The 4th field has the timestamp information we need
    # and the 10th field has the bytes information we need.

    # Split the date into an array and reformulate into something that
    # is precise to the minute.
    split($4, parts, /[/:\[]/)
    datetime = sprintf("%s/%s/%s:%s", parts[2], parts[3], parts[4], parts[5])

    # Populate the IP buckets.
    ip = $1
    numbytes = $10
    count[datetime][ip] += numbytes
    hits_count[datetime][ip] += 1

} END {

    # Print the IP address with with largest bandwidth in each minute bucket.

    for (minute in count) {
        max = 0
        max_ip = ""
        for (ip in count[minute]) {
            if (max < count[minute][ip]) {
                max = count[minute][ip]
                max_ip = ip
            }
        }
        printf(body_pspec, minute, max_ip, max / (1024 * 1024))
    }
}

