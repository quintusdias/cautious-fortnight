#!/bin/bash

# Who has the most hits for each minute?
# The input streams in via stdin, assumed to be an apache log.
# The output is not sorted.
#
# Example usage:
# 
#   zcat apache_log.gz | awk -f most_hits_by_minute.awk | sort -k1,1

BEGIN {

    # If we set OFS to a comma, then the implication is that the output
    # is a CSV file.
    if ( OFS == "," ) {
        PRINT_SPEC = "%s,%s,%.2f\n"
    } else {
        PRINT_SPEC = "%20s %20s %10.2f\n"
    }

}

{

    # The first item in an apache log file entry is the IP address.
    ip = $1

    # The date is the 4th item in an apache log file entry is the date.
    # It will look something like "07/Mar/2018:20:00:00".
    # Split it into an array (fieldseps are "/" and ":").
    split($4, parts, /[/:\[]/)

    # Reformulate something like 07/Mar/2018:20:00:00 into
    # 07/Mar/2018:20:00, i.e. get rid of the seconds.
    minute = sprintf("%s/%s/%s:%s:%s", parts[4], parts[3], parts[2], parts[5], parts[6])

    # Accumulate the count for the minute and the IP address.
    count[minute][ip] += 1

}

END {

    # Find the largest count in each minute bucket by IP address.
    for (minute in count) {

        max = 0
        max_ip = ""
        for (ip in count[minute]) {
            if (max < count[minute][ip]) {
                max = count[minute][ip]
                max_ip = ip
            }
        }

        largest_ip_by_minute[minute] = max_ip
        max_count_by_minute[minute] = max

    }

    # Sort the indices, not the data, because we still need the original indices.
    n = asorti(largest_ip_by_minute, minutes)
    for (i = 1; i <= n; ++i) {
        printf(PRINT_SPEC, minutes[i], largest_ip_by_minute[minutes[i]], max_count_by_minute[minutes[i]])
    }

}

