#!/usr/bin/awk
#
# Count the User-Agents
#
# Example:  zcat apache_log.gz | awk -f most_user_agents.awk | head
BEGIN {
    # We need to full describe the regular expression for the fields instead
    # of letting awk split the record itself.
    # 
    #   66.249.66.180 - - [01/Apr/2016:00:00:18 +0000] "GET something HTTTP/1.1" 200 - "-" "a browser"
    #
    # "[^ ]+"       - matches most fields
    # \"[^\"]+\"    - matches fields beginning and ending with double quotes
    # \\[[^\\]]+\\] - matches the time field
    FPAT = "([^ ]+)|(\"[^\"]+\")|(\\[[^\\]]+\\])"
}

{
    PROCINFO["sorted_in"] = "@val_num_desc"
    user_agent = $9
    user_agents_count[user_agent] += 1
}

END {
    # Print the user agents and the count.  Note that this is unsorted.
    for (ua in user_agents_count) {
        printf "%40s %10s\n", substr(ua, 1, 40), user_agents_count[ua]
    }
}


