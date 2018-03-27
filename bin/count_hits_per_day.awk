# Count the hits per day.
#
{
    # For an apache record like the following...
    #
    # 174.194.5.135 - - [21/Mar/2018:15:58:52 +0000] "GET  ..."
    # 
    # $4 ==> [21/Mar/2018:15:58:52
    datestring = substr($4, 2, 11)

    split(datestring, parts, "[/:]")
    day = parts[1]
    monthstr = parts[2]
    year = parts[3]

    switch (monthstr) {
        case "Jan":
            month = "01"
            break
        case "Feb":
            month = "02"
            break
        case "Mar":
            month = "03"
            break
        case "Apr":
            month = "04"
            break
        case "May":
            month = "05"
            break
        case "Jun":
            month = "06"
            break
        case "Jul":
            month = "07"
            break
        case "Aug":
            month = "08"
            break
        case "Sep":
            month = "09"
            break
        case "Oct":
            month = "10"
            break
        case "Nov":
            month = "11"
            break
        case "Dec":
            month = "12"
            break
    }
    #date = mktime(sprintf("%s %s %s 0 0 0", year, month, day))
    #count[date] += 1
    date = sprintf("%s-%s-%s", year, month, day)
    count[date] += 1
}

END {
    for (dt in count) {
        printf "%12s %20s\n", dt, count[dt]
    }
}

