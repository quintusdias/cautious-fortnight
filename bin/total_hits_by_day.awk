#!/usr/bin/awk
#
# Count the hits for each day
#
# Example:  zcat apache_log.gz | awk -f most_hits.akw | sort -nk2,2 -r
{
    time_field = $4
    day = substr(time_field, 2, 11)
    count[day] += 1
} END {
    for (day in count) {
        printf "%15s %15s %10.1f\n", day, count[day], count[day]/86400
    }
}

