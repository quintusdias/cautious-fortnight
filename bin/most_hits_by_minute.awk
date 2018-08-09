#!/bin/bash

# Who has the most hits for each minute?
# The input streams in via stdin, assumed to be an apache log.
# The output is not sorted.
#
# Example usage:
# 
#   zcat apache_log.gz | awk -f most_hits_by_minute.awk | sort -k1,1

{
    ip = $1
    # Split the date into an array.
    split($4, parts, /[/:\[]/)
    # Reformulate something like 07/Mar/2018:20:00:00 into
    # 07/Mar/2018:20:00, i.e. get rid of the seconds.
    minute = sprintf("%s/%s/%s:%s:%s", parts[2], parts[3], parts[4], parts[5], parts[6])
    count[minute][ip] += 1
} END {
    for (minute in count) {
        max = 0
        max_ip = ""
        for (ip in count[minute]) {
            if (max < count[minute][ip]) {
                max = count[minute][ip]
                max_ip = ip
            }
        }
        printf "%20s %20s %10.2f\n", minute, max_ip, max
    }
}

