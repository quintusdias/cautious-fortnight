#!/usr/bin/awk
#
# Count the hits for each IP address.
#
# Example:  zcat apache_log.gz | awk -f most_hits.akw | sort -nk2,2 -r
BEGIN {

    header_pspec = "%s,%s,%s,%s,%s\n"
    body_pspec = "%s,%s,%d,%d,%.2f\n"

    # Header line
    printf(header_pspec, "Time", "IP", "Hits", "Errors", "Bytes")

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

}

{
    # We are interested in three fields here.  The 11th field has the
    # referer address.  The 4th field has the timestamp information we need
    # and the 10th field has the bytes information we need.
    ip = $1
    datestr = process_datestr($4)
    status_code = $9
    numbytes = $10

    # Populate the referer buckets.
    if ((status_code > 0) && (status_code < 300)) {
        count[datestr][ip]["bytes"] += numbytes
    } else {
        count[datestr][ip]["errors"] += 1
    }		
    count[datestr][ip]["hits"] += 1

} 

END {

    for (time in count) {
        for (ip in count[time]) {
            nbytes = count[time][ip]["bytes"]
            nerrors = count[time][ip]["errors"]
            nhits = count[time][ip]["hits"]
            printf(body_pspec, time, ip, nhits, nerrors, nbytes)
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

