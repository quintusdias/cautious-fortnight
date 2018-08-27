#!/usr/bin/env python

"""
Find which IP address is responsible for the most hits during a specific time
frame.
"""

# Standard library imports
import argparse
import collections
import io
import sys

# 3rd party library imports
import pandas as pd


class MostHitsPerMinute(object):
    """
    Attributes
    ----------
    df : pandas DataFrame
        Records the minute, the number of hits and the IP address responsible
        for that number of hits.
    infile, outfile : file
        Input taken from here (could be stdin) and output written to here.
    csv : bool
        If true, write the output to a CSV file.
    period : str
        Use either 'minutes' or 'seconds' for the width of the buckets (bins).
    time_slice : slice
        Depending on what the period is, use this to slice out the date
        information from the apache log file entries.
    date_format : str
        Depending on what the period is, use this to convert date strings into
        python dates.
    """
    def __init__(self, infile, outfile, csv=None, period=None):
        self.infile = infile
        self.outfile = outfile
        self.csv = csv
        self.period = period

        if period == 'minutes':
            self.columns = ['Date', 'IP', 'Hits/minute']

            # The relevant field in the apache log file looks something like
            #
            #   "[23/Aug/2018:15:31:19"
            #
            # In the case of minutes, strip off that leading "[" and remove the
            # seconds since we are accumulating by the minute.
            self.time_slice = slice(1, -3)

            self.date_format = '%d/%b/%Y:%H:%M'

        else:
            self.columns = ['Date', 'IP', 'Hits/second']

            # Just slice off that "[" from the front.
            self.time_slice = slice(1, None)

            self.date_format = '%d/%b/%Y:%H:%M:%S'

    def run(self):

        self.compute_max_ip_by_minute()

        if self.csv:
            self.df.to_csv(self.outfile)
        else:
            # print the dataframe to the given output file (probably
            # sys.stdout???)
            print(self.df, file=self.outfile)

    def compute_max_ip_by_minute(self):

        items = {}

        for line in self.infile:

            parts = line.split()

            ip_address = parts[0]

            # extract the time bucket.
            datestr = parts[3][self.time_slice]

            if datestr not in items.keys():
                items[datestr] = collections.Counter()
            items[datestr][ip_address] += 1

        # OK, go thru each minute, take the largest count by IP address.
        records = []
        for datestr, item in items.items():
            ip, count = items[datestr].most_common(1)[0]
            records.append((datestr, ip, count))

        df = pd.DataFrame(records, columns=self.columns)
        df.sort_values(by='Date', inplace=True)

        # Turn the date strings into real python dates and create an index out
        # of it.
        df['Date'] = pd.to_datetime(df['Date'], format=self.date_format)
        self.df = df.set_index('Date')


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # If no infile argument is given, then assume standard input (i.e. like a
    # pipe).  We need to wrap sys.stdin because spammers often throw bogus
    # non-utf8 characters into the requests, and sys.stdin has strict handling
    # by default that would otherwise error out.
    help = 'Take input from this file.'
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'),
                        default=io.TextIOWrapper(sys.stdin.buffer,
                                                 errors='replace'),
                        help=help)

    help = 'Write output to this file.'
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'),
                        default=sys.stdout, help=help)

    help = 'Write output to CSV file.'
    parser.add_argument('--csv', action='store_true', help=help)

    help = 'Time period'
    parser.add_argument('--period', choices=['minutes', 'seconds'],
                        default='minute', help=help)

    args = parser.parse_args()

    o = MostHitsPerMinute(args.infile, args.outfile,
                          csv=args.csv, period=args.period)
    o.run()
