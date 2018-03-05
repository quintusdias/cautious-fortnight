import re

import pandas as pd

def read_csv(csvfile):
    """
    Parameters
    ----------
    csvfile : path or str
        Path to CSV file downloaded from Akamai Luna console
    """
    kwargs = {}

    # Need to determine a few things from the header section of the CSV file
    # before reading it in.  We need the column names, the number of lines
    # to skip in order to read the data, and the total number of lines to read.
    last_line = ''
    for idx, line in enumerate(open(csvfile, mode='rt')):
        if line.startswith('# COLUMN_DEFINITION_END'):
            # the last line has the info we need on the columns
            # Strip any quotes and newlines.
            pattern = """
                      ['"\n]
                      """
            regex = re.compile(pattern, re.VERBOSE)
            line = regex.sub('', last_line)
            kwargs['names'] = line.split(',')

        if line.startswith('# ROW_DATA_START'):
            row_start = idx + 1

        if line.startswith('# ROW_DATA_END'):
            row_end = idx + 1

        # set the last line to the current line so that we keep history.
        last_line = line

    if 'Time' in kwargs['names']:
        kwargs['parse_dates'] = [kwargs['names'].index('Time')]
        kwargs['index_col'] = 'Time'
    else:
        kwargs['parse_dates'] = False
        kwargs['index_col'] = None

    kwargs['skiprows'] = row_start
    nrows = row_end - row_start - 1
    kwargs['nrows'] = nrows

    df = pd.read_csv(csvfile, **kwargs)
    return df
