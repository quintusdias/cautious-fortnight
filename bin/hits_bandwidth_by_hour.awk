#!/usr/bin/awk
#
# Calculate the bandwidth for each day
#
# Example:  zcat apache_log.gz | awk -f hits_bandwidth_by_day.awk | sort -nk2,2 -r
BEGIN {
    # We need to map the 3-char month strings into ordinal numbers because we
    # will need them to properly construct UNIX timestamps at the end.
    split("Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec", monthstrs, "|")
    for (i = 1; i <= 12; ++i) {
        month[monthstrs[i]] = i
    }

    header_pspec = "%11s%15s%15s%10s%12s%10s\n"
    body_pspec = "%11s%15s%15s%10.1f%12s%10.1f\n"

    # Header line
    printf(header_pspec, "Day", "TS", "Bytes", "GB/hr", "Hits", "Hits/sec")
}

{
    # The 4th field will look something like "[01/Mar/2017:00:00:00" when we
    # split on the space character.  Cut off the leading bracket and trailing
    # hour:min:sec part.
    time_field = $4
    hour = substr(time_field, 2, 14)

    bytes[hour] += $10
    hits[hour] += 1
} 

END {
    for (hour in hits) {
        
        # Convert the time string (something like "01/Mar/2017:18") into a unix
        # timestamp.
        split(hour, parts, /\/|:/)
        spec = parts[3] " " month[parts[2]] " " parts[1] " " parts[4] " 0 0"
        ts = mktime(spec)

	bandwidth_rate = bytes[hour]/1024/1024/1024
        printf(body_pspec, hour, ts, bytes[hour], bandwidth_rate, hits[hour], hits[hour]/3600)
    }
}

