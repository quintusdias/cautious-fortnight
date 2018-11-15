#!/usr/bin/env python

"""
Aggregate all users to two-minute averages and 5-minute bursts.  Save the
output to pandas HDFStore.
"""

# Standard library imports
import argparse
import collections
import datetime as dt
import io
import sys
import time

# 3rd party library imports
import pandas as pd


class MostBandwidth(object):
    """
    Attributes
    ----------
    df : pandas DataFrame
        Records the time, the number of hits and the IP address responsible
        for that number of hits.
    infile, outfile : file
        Input taken from here (could be stdin) and output written to here.
    date_format : str
        Depending on what the period is, use this to convert date strings into
        python dates.
    """
    def __init__(self, infile, outfile):
        self.infile = infile
        self.outfile = outfile

    def run(self):

        self.bin_to_seconds()

        self.aggregate_to_bursts()
        self.aggregate_to_two_minute_average()

        # Store the results.
        with pd.HDFStore(self.outfile) as store:
            store['burst'] = self.burst
            store['average'] = self.average

    def aggregate_to_two_minute_average(self):
        """
        Aggregate the bins to two minutes.
        """
        # Calculate the bins for the burst series.  These should be every two
        # minutes.
        avg_minutes = [ts.minute // 2 * 2 for ts in self.time_index]
        avg_time = [
            pd.Timestamp(*(ts.timetuple()[:4]), minute, 0)
            for ts, minute in zip(self.time_index, avg_minutes)
        ]

        df = pd.DataFrame({'time': avg_time, 'IP': self.ip, 'hits': self.hits})

        # Sum the hit counts, but get an average rate of hits per second
        self.average = df.groupby(['time', 'IP']).sum()  / 120

    def aggregate_to_bursts(self):
        """
        Aggregate the bins to 5 second bursts.
        """
        # Calculate the bins for the burst series.  These should be every 5
        # seconds.
        burst_seconds = [ts.second // 5 * 5 for ts in self.time_index]
        burst_time = [
            pd.Timestamp(*(ts.timetuple()[:5]), second)
            for ts, second in zip(self.time_index, burst_seconds)
        ]

        df = pd.DataFrame({'time': burst_time, 'IP': self.ip, 'hits': self.hits})

        # Sum the hit counts, but get a burst rate of hits per second
        self.burst = df.groupby(['time', 'IP']).sum() / 5

    def bin_to_seconds(self):
        """
        For each log item, take note of the timestamp and the IP address.  Add
        to the counter at each timestamp for that IP address.

        At the end of this method, we have constructed lists of the hit count
        corresponding to a datestring (timestamp to the second) and IP address.
        """

        # rates is a two-level dictionary.  The first level is the date, the
        # second level is the IP address.
        rates = {}

        for line in self.infile:

            parts = line.split()

            # Parse out the date string and IP address.
            ip_address = parts[0]
            datestr = parts[3][1:]

            if datestr not in rates.keys():
                rates[datestr] = collections.Counter()
            rates[datestr][ip_address] += 1

        # Now flatten out the structure.
        datestr_ip, self.hits = zip(*[((i, j), rates[i][j]) for i in rates for j in rates[i]])

        # Convert the (datestr, IP) tuples into (datetime, IP) tuples
        self.time_index = pd.to_datetime([t[0] for t in datestr_ip],
                                         format='%d/%b/%Y:%H:%M:%S')
        self.ip = [t[1] for t in datestr_ip]


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # If no infile argument is given, then assume standard input (i.e. like a
    # pipe).  We need to wrap sys.stdin because spammers often throw bogus
    # non-utf8 characters into the requests, and sys.stdin has strict handling
    # by default that would otherwise error out.
    help = 'Write output to this HDF5 file.'
    parser.add_argument('outfile', type=str, help=help)

    help = 'Take input from this file.  Usually this should be stdin.'
    parser.add_argument('--infile', nargs='?', type=argparse.FileType('r'),
                        default=io.TextIOWrapper(sys.stdin.buffer,
                                                 errors='replace'),
                        help=help)

    args = parser.parse_args()

    o = MostBandwidth(args.infile, args.outfile)
    o.run()
