#!/usr/bin/env python

# Standard library imports
import collections
import argparse
import sys

# 3rd party library imports
import pandas as pd


class MostHitsPerMinute(object):
    """
    Attributes
    ----------
    infile, outfile : file
        Input and output taken from here.  "infile" could be stdin.
    csv : bool
        If true, write the output to a CSV file.
    """
    def __init__(self, infile, outfile, csv):
        self.infile = infile
        self.outfile = outfile
        self.csv = csv

    def run(self):

        self.compute_max_ip_by_minute()

        if self.csv:
            self.df.to_csv(outfile)
        else:
            # print the dataframe to the given output file (probably
            # sys.stdout???)
            print(self.df, file=self.outfile)
    

    def compute_max_ip_by_minute(self):

        items = {}

        buffer_reader = self.infile.buffer
        for line in buffer_reader:

            try:
                line = line.decode('utf-8').strip()
            except UnicodeDecodeError:
                print(line)
                continue

            parts = line.split()

            ip_address = parts[0]

            # The fourth argument starts out looking like
            #
            #   "[23/Aug/2018:15:31:19"
            #
            # So strip off that leading "[" and remove the seconds since we are
            # accumulating by the minute.
            datestr = parts[3][1:-3]

            if datestr not in items.keys():
                items[datestr] = collections.Counter()
            items[datestr][ip_address] += 1

        # OK, go thru each minute, take the largest count by IP address.
        records = []
        for datestr, item in items.items():
            largest = items[datestr].most_common(1)
            records.append((datestr, largest[0][0], largest[0][1]))

        df = pd.DataFrame(records, columns=['Date', 'IP', 'Count']) 
        df.sort_values(by='Date', inplace=True)
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%b/%Y:%H:%M')
        self.df = df.set_index('Date')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', action='store_true')
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin)
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'),
                        default=sys.stdout)
    args = parser.parse_args()

    o = MostHitsPerMinute(args.infile, args.outfile, args.csv)
    o.run()

