#!/usr/bin/awk
#
# Count the hits for each IP address.
#
# Example:  zcat apache_log.gz | awk -f most_hits.akw | sort -nk2,2 -r
BEGIN {
    line_spec = "%40s %20s\n"
    printf(line_spec, "IP", "Count")
}

{
    ip = $1
    count[ip] += 1
} 

END {
    for (ip in count) {
        printf(line_spec, ip, count[ip])
    }
}

