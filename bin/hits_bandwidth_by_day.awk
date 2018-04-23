#!/usr/bin/awk
#
# Calculate the bandwidth for each day
#
# Example:  zcat apache_log.gz | awk -f most_hits.akw | sort -nk2,2 -r
BEGIN {
    # We need to map the 3-char month strings into ordinal numbers because we
    # will need them to properly construct UNIX timestamps at the end.
    split("Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec", monthstrs, "|")
    for (i = 1; i <= 12; ++i) {
        month[monthstrs[i]] = i
    }

    # Header line
    printf("%11s%15s%15s%10s%12s%10s\n", "Day", "TS", "Bytes", "GB/day", "Hits", "Hits/sec")
}

{
    # The 4th field will look something like "[01/Mar/2017:00:00:00" when we
    # split on the space character.  Cut off the leading bracket and trailing
    # hour:min:sec part.
    time_field = $4
    day = substr(time_field, 2, 11)

    bytes[day] += $10
    hits[day] += 1
} 

END {
    for (day in hits) {
        
        # Convert the day string (something like "01/Mar/2017") into a unix
        # timestamp.
        split(day, dmy, "/")
        spec = dmy[3] " " month[dmy[2]] " " dmy[1] " 0 0 0"
        ts = mktime(spec)

        printf("%11s%15s%15s%10.1f%12s%10.1f\n", day, ts, bytes[day], bytes[day]/1024/1024/1024, hits[day], hits[day]/86400)
    }
}

