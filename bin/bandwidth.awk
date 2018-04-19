#!/bin/bash
#
# Determine total bandwidth for an apache log file.
# This is currently too slow to crunch an entire daily nowcoast apache file.
#
# Example usage:
# 
#   zcat apache_log.gz | awk -f bandwidth.awk | sort -r -kn2,2 | head 10


{
    # The IP address is in the first column.
    # The number of bytes is in the 10th column.#
    ip = $1
    sum[ip] += $10
} 

END {
    # Print out the totals in GB.
    for (ip in sum) {
        printf "%20s %20.4f\n", ip, sum[ip] / 1024 / 1024 / 1024
    }
}


