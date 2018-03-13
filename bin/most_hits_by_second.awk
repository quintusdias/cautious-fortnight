#!/bin/bash

# Who has the most hits for each second?
# The input streams in via stdin, assumed to be an apache log.
# The output is not sorted.
#
# Example usage:
# 
#   zcat apache_log.gz | awk -f most_hits_per_second.awk | sort -k1,1

{
    ip = $1
    second = $4
    count[second][ip] += 1
} END {
    for (second in count) {
        max = 0
        max_ip = ""
        for (ip in count[second]) {
            if (max < count[second][ip]) {
                max = count[second][ip]
                max_ip = ip
            }
        }
        printf "%20s %20s %10.2f\n", second, max_ip, max
    }
}

