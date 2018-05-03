# We wish to filter postgresql logs.  There is a pattern that exists 
# across consecutive lines that we wish to filter out.  Use two regular
# expressions and keep track of the previous line before making a
# decision about printing out a line or not.

BEGIN{
    IGNORECASE = 1

    # This regular expression should match a line looking like the following:
    #
    #     2018-04-27 13:00:04.595 UTC 10.9.96.4(59380) [24982]: [5ae30ba8.6196-0]:  
    #     [5517-1] 42703  user=nowcoast,db=nowcoast 
    #     ERROR:  column "starttime" does not exist at character 8
    #
    last_regexp = "column \"(starttime|latest)\" does not exist at character [[:digit:]]+"

    # This regular expression should match a line looking like the following:
    #
    #     2018-04-27 13:00:01.205 UTC 10.9.96.4(8193) 
    #     [10024]: [5ae2ff97.2728-0]:  [5422-1] 42703  user=nowcoast,db=nowcoast
    #     STATEMENT:  SELECT starttime,endtime FROM nowcoast.nowcoast.amd_mrms_basereflect_bnd
    #
    current_regexp = "select .* from .*_bnd"
}

{

    if ($0 ~ current_regexp) {
        if (lastline ~ last_regexp) {
            # This is the pattern we want to throw out.
            # Don't print the previous line.
            lastline = "THROWNOUT"
            next
        }
    }

    # So at least one regular expression failed.  Should we print the
    # previous line?
    if (NR > 1) {
        # We shouldn't print the very first line.
        if (lastline !~ /THROWNOUT/) {
            printf("%s\n", lastline)
        }
    }

    # Last step is to save the current line.
    lastline = $0
}

END {
    if (lastline !~ /THROWNOUT/) {
        printf("%s\n", lastline)
    }
}
