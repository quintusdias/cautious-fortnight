#!/bin/bash
#
# Determine who refers the most bandwidth by the day
#
# Example usage:
# 
#   zcat apache_log.gz | awk -f most_referer_bandwidth_by_day.awk | sort -k1,1
#
# This may produce output that looks as follows:
#
#               Date           IP Address         MB    
#  09/Jul/2018:00:00       66.223.179.138      13.66
#  09/Jul/2018:00:01       66.223.179.138      14.13
#  09/Jul/2018:00:02        216.38.80.221      15.29
#
#

BEGIN {

    header_pspec = "%20s%60s%10s\n"
    body_pspec = "%20s%60s%10.2f\n"

    # Header line
    printf(header_pspec, "Date", "Referer", "MB")
}

{
    # We are interested in three fields here.  The 11th field has the
    # referer address.  The 4th field has the timestamp information we need
    # and the 10th field has the bytes information we need.

    # Split the date into an array and reformulate into something that
    # is precise to the day.
    split($4, parts, /[/:\[]/)
    datetime = sprintf("%s/%s/%s", parts[2], parts[3], parts[4])

    # Populate the referer buckets.
    referer = $11
    numbytes = $10
    count[datetime][referer] += numbytes

} END {

    # Print the referer with with largest bandwidth in each day bucket.

    for (day in count) {
        max = 0
        max_referer = ""
        for (referer in count[day]) {
            nbytes = count[day][referer]
            printf(body_pspec, day, referer, nbytes / (1024 * 1024))
        }
    }
}

