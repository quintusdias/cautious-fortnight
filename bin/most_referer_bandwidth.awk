#!/bin/bash
#
# Determine who refers the most bandwidth by the day
#
# Example usage:
# 
#   zcat apache_log.gz | awk -f most_referer_bandwidth_by_day.awk | sort -k1,1
#
# This may produce output that looks as follows:
#
#               Date           IP Address         MB    
#  09/Jul/2018:00:00       66.223.179.138      13.66
#  09/Jul/2018:00:01       66.223.179.138      14.13
#  09/Jul/2018:00:02        216.38.80.221      15.29
#
#

BEGIN {

    header_pspec = "%s,%s,%s,%s,%s\n"
    body_pspec = "%s,%s,%d,%d,%.2f\n"

    # Header line
    printf(header_pspec, "Date", "Referer", "Hits", "Errors", "MB")

    if (ENVIRON["PRECISION"] == "days") 
        dateformat = "%Y-%m-%d";
    else if (ENVIRON["PRECISION"] == "hours") 
        dateformat = "%Y-%m-%dT%H";
    else if (ENVIRON["PRECISION"] == "minutes") 
        dateformat = "%Y-%m-%dT%H:%M";
    else if (ENVIRON["PRECISION"] == "seconds") 
        dateformat = "%Y-%m-%dT%H:%M:%S";
    else 
        dateformat = "%Y-%m-%d";

    # Some referers have geographic information embedded, such as
    #     https://avcamsplus.faa.gov/map/-154.74409,59.51432,-143.8896,62.57772
    # Remove such geographic information because all we are really interested
    # in this case is https://avcamsplus.faa.gov/map/
    numpattern = "[+-]?[0-9]+.[0-9]+"
    geog_pattern = sprintf("%s,%s,%s,%s", numpattern, numpattern, numpattern, numpattern)
}

{
    # We are interested in three fields here.  The 11th field has the
    # referer address.  The 4th field has the timestamp information we need
    # and the 10th field has the bytes information we need.
    datestr = process_datestr($4)
    referer = process_referer($11)
    status_code = $9
    numbytes = $10

    # Populate the referer buckets.
    if ((status_code > 0) && (status_code < 300)) {
        count[datestr][referer]["bytes"] += numbytes
    } else {
        count[datestr][referer]["errors"] += 1
    }		
    count[datestr][referer]["hits"] += 1

} 

END {

    for (time in count) {
        for (referer in count[time]) {
            nbytes = count[time][referer]["bytes"]
            nerrors = count[time][referer]["errors"]
            nhits = count[time][referer]["hits"]
            printf(body_pspec, time, referer, nhits, nerrors, nbytes)
        }
    }
}

function convert_month(month) {
    return(((index("JanFebMarAprMayJunJulAugSepOctNovDec", month) - 1) / 3) + 1)

}

function process_datestr(datestr) {

    # Split the date into an array and reformulate into something that
    # is precise to the day.
    split(datestr, parts, /[/:\[]/)
    Y = parts[4]
    M = convert_month(parts[3])
    D = parts[2]
    h = parts[5]
    m = parts[6]
    s = parts[7]
    timestamp = mktime(sprintf("%d %d %d %d %d %d", Y, M, D, h, m, s))

    return strftime(dateformat, timestamp)

}

function process_referer(referer) {

    # Remove any parameters from the referer field.  This might remove
    # a trailing double quote, so temporarily remove any leading double
    # quote as well.
    split(referer, parts, /[?;]/)
    referer = gensub(/"/, "", "g", parts[1])
    # Get rid of any geography pattern in the referer.
    referer = gensub(geog_pattern, "", "g", referer)

    # Add the quotes back in.  This way we always get a fully double-quoted referer.
    return "\""referer"\""
}
