#!/usr/bin/env python3

"""
There is a two-line pattern in the nowCOAST postgresql logs that we need to
filter.  Something like

    First line:        2018-04-27 13:00:04.596 UTC 10.9.96.4(62733) [11989]: 
                       [5ae300f3.2ed5-0]:  [4301-1] 42703  
                       user=nowcoast,db=nowcoast ERROR:  
                       column "latest" does not exist at character 116

    Following line:    2018-04-27 13:00:04.596 UTC 10.9.96.4(62733) [11989]: 
                       [5ae300f3.2ed5-0]:  [4302-1] 42703  
                       user=nowcoast,db=nowcoast STATEMENT:  
                       select  amd_mrms_basereflect_bnd.shape,  objectid  from  
                       nowcoast.nowcoast.amd_mrms_basereflect_bnd   
                       where (( ( ( latest=1 ) ) ) AND 
                       ( NOT ( ( starttime > timestamp '2018-04-27 13:00:03' 
                       and starttime is not null) or 
                       ( endtime < timestamp '2018-04-27 13:00:03' and 
                       endtime is not null ) ) ))
"""

import re
import sys

def run(fp):
    pattern = """
              column\s"(starttime|latest)"\s
              does\snot\sexist\sat\scharacter\s\d+
              """
    last_regex = re.compile(pattern, re.VERBOSE | re.IGNORECASE)

    pattern = """
              select\s+.*\s+from\s+
              .*_bnd
              """
    current_regex = re.compile(pattern, re.VERBOSE | re.IGNORECASE)
    
    last_line = ""

    for current_line in fp:
        if current_regex.search(current_line):
            if last_regex.search(last_line):
                # We matched the pattern over two lines.  Throw both out.
                last_line = ""
                continue

        if last_line != "":
            print(last_line.rstrip())

        last_line = current_line

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as fp:
            run(fp)
    else:
        run(sys.stdin)
