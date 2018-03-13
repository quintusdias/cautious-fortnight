#!/usr/bin/awk
#
# Count the hits for each IP address.
#
# Example:  zcat apache_log.gz | awk -f most_hits.akw | sort -nk2,2 -r
{
    ip = $1
    count[ip] += 1
} END {
    for (ip in count) {
        printf "%20s %20s\n", ip, count[ip]
    }
}

