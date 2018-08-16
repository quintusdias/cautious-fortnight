#!/usr/bin/awk
#
# Calculate the bandwidth for each day
#
# Example:  zcat apache_log.gz | awk -f hits_bandwidth_by_day.awk | sort -nk2,2
#
# This produces output that may look something like
#
#        Day             TS          Bytes     GB/hr        Hits  Hits/sec
# 09/Jul/2018:00     1531094400     9278510770       8.6      681689     189.4
# 09/Jul/2018:01     1531098000     7587381047       7.1      668871     185.8
# 09/Jul/2018:02     1531101600     6981265678       6.5      607839     168.8
# 09/Jul/2018:03     1531105200     6181813703       5.8      542619     150.7
# 
BEGIN {
    # We need to map the 3-char month strings into ordinal numbers because we
    # will need them to properly construct UNIX timestamps at the end.
    split("Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec", monthstrs, "|")
    for (i = 1; i <= 12; ++i) {
        month[monthstrs[i]] = i
    }

    header_pspec = "%17s%15s%15s%10s%12s%10s\n"
    body_pspec = "%17s%15s%15s%10.1f%12s%10.1f\n"

    # Header line
    printf(header_pspec, "Date", "TS", "Bytes", "MB/min", "Hits", "Hits/sec")
}

{
    # We are interested in two fields here.  The 4th field has the timestamp
    # information we need and the 10th field has the bytes information we need.

    # The 4th field will look something like "[01/Mar/2017:00:00:00" when we
    # split on the space character.  Cut off the leading bracket and trailing
    # :sec part.
    time_field = $4
    minute = substr(time_field, 2, 17)

    # For each record, increment the bytes count and hits count for the current
    # timestamp.
    bytes[minute] += $10
    hits[minute] += 1
} 

END {

    # Write the summary records, one per hour.

    for (minute in hits) {
        
        # Convert the time string (something like "01/Mar/2017:18") into a unix
        # timestamp.
        split(minute, parts, /\/|:/)
        spec = parts[3] " " month[parts[2]] " " parts[1] " " parts[4] " " parts[5] " 0"
        ts = mktime(spec)

	    bandwidth_rate = bytes[minute]/1024/1024
        printf(body_pspec, minute, ts, bytes[minute], bandwidth_rate, hits[minute], hits[minute]/60)
    }
}

