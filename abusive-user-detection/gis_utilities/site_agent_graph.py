"""
Plot the top sites percentages, top agents percentages, and overall daily hits.
"""
# Standard library imports ...
import datetime as dt
import glob
import heapq
import os
import pathlib
import socket

# Third party library imports ...
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SITES_FILES_FMT = '{home}/data/webalizer/{origin}/{project}/dump/*/site*.csv'


def smallest_unique_set(s):
    max_string_len = max(map(len, s))

    idx = 1
    while idx < max_string_len:
        candidates = set([item[:idx] for item in s])
        if len(s) == len(candidates):
            break
        idx += 1
    return idx


class WebalizerAgents(object):
    """
    Graph of the top user agents.
    """

    def __init__(self, origin, project):
        self.origin = origin
        self.project = project
        self.pattern = (f'{pathlib.Path.home()}/data/webalizer/{origin}'
                        f'/{project}/dump/*/agent*.csv')

        self.NUM_AGENTS = 10
        self.NDAYS = 60

    def run(self, ax):

        # Acqu
        filelist = glob.glob(self.pattern)

        # Limit to the twenty most recent.
        filelist = heapq.nlargest(self.NDAYS, filelist, key=os.path.getctime)

        df = pd.read_csv(filelist[0], sep='\t')

        # Get the top agents.
        agents = df['User Agent'][:self.NUM_AGENTS].values

        # Create "smallest" labels for the agents.  Otherwise the user agent
        # strings are too bulky to effectively use.
        maxlen = smallest_unique_set(agents)
        agent_labels = [s[:maxlen] for s in agents]

        # Create the dataframe index.  This is a time series of the days used.
        dates = []
        for file in filelist:
            daystring = pathlib.Path(file).parts[-2]
            date_parts = tuple(int(p) for p in daystring.split('-'))
            dates.append(dt.date(*date_parts))
        index = pd.DatetimeIndex(dates)

        # Force the datatype of the columns to be floating point.
        agents_df = pd.DataFrame(columns=agents, index=index, dtype=np.float64)

        for file, date in zip(filelist, index):
            print(file, date)
            df = pd.read_csv(file, sep='\t')

            total_hits = df.sum()['Hits']

            # Locate the agent traffic for the last few days
            for agent in agents:
                try:
                    hits = df[df['User Agent'] == agent]['Hits'].values[0]
                except IndexError:
                    continue
                agents_df.loc[date, agent] = hits / total_hits * 100

        # Flip the data up/down.  Gets the dates in the right order.
        agents_df.rename(columns=dict(zip(agents, agent_labels)), inplace=True)
        df = agents_df.iloc[::-1]

        df.plot(ax=ax, title='User Agents')
        ax.set_ylabel('Percent')


class WebalizerSites(object):
    """
    Plot the sites responsible for the most traffic over the past few days.
    """
    def __init__(self, origin, project):
        self.origin = origin
        self.project = project

    def run(self, ax):
        pattern = SITES_FILES_FMT.format(home=pathlib.Path.home(),
                                         origin=self.origin,
                                         project=self.project)
        filelist = glob.glob(pattern)

        # Limit to the ten most recent.
        filelist = heapq.nlargest(20, filelist, key=os.path.getctime)

        latest_df = pd.read_csv(filelist[0], sep='\t')

        # Get the top ten sites.
        sites = latest_df.Hostname[:10].values

        # Try to resolve the IP addresses.
        sites_ip = sites.copy()
        lst = []
        for site in sites:
            try:
                hostname, _, _ = socket.gethostbyaddr(site)
            except Exception as e:
                hostname = site
            if hostname == '.':
                # Is this nLayer?
                hostname = site + ':nLayer???'
            lst.append(hostname)
        sites = lst

        # Create the dataframe index
        dates = []
        for file in filelist:
            daystring = pathlib.Path(file).parts[-2]
            date_parts = tuple(int(p) for p in daystring.split('-'))
            dates.append(dt.date(*date_parts))
        index = pd.DatetimeIndex(dates)

        top_sites = pd.DataFrame(columns=sites, index=index, dtype=np.float64)

        for file, date in zip(filelist, index):
            print(file, date)
            df = pd.read_csv(file, sep='\t')

            total_hits = df.sum()['Hits']

            # Locate the site traffic for the top ten sites.
            for hostname, ip in zip(sites, sites_ip):
                try:
                    hits = df[df.Hostname == ip]['Hits'].values[0]
                except IndexError:
                    continue
                top_sites.loc[date, hostname] = hits / total_hits * 100

        # Flip the data up/down.  Gets the dates in the right order.
        df = top_sites.iloc[::-1]

        df.plot(ax=ax, title='Sites')
        ax.set_ylabel('Percent')


class WebalizerTraffic(object):
    """
    Plot the daily traffic.

    Attributes
    ----------
    s : pandas.Series
        Time series of total daily traffic
    """
    def __init__(self, origin, project):
        self.origin = origin
        self.project = project
        self.NDAYS = 60

    def run(self, ax):
        pattern = SITES_FILES_FMT.format(home=pathlib.Path.home(),
                                         origin=self.origin,
                                         project=self.project)
        filelist = glob.glob(pattern)

        # Limit to the most recent.
        filelist = heapq.nlargest(self.NDAYS, filelist, key=os.path.getctime)

        # Create the dataframe index
        dates = []
        for file in filelist:
            print(file)
            daystring = pathlib.Path(file).parts[-2]
            date_parts = tuple(int(p) for p in daystring.split('-'))
            dates.append(dt.date(*date_parts))
        index = pd.DatetimeIndex(dates)

        s = pd.Series(index=index, dtype=np.float64)

        for file, date in zip(filelist, index):
            df = pd.read_csv(file, sep='\t')

            s.loc[date] = df.sum()['Hits']

        # Flip the data up/down.  Gets the dates in the right order.
        s = s.iloc[::-1]
        # self.s = s.reindex(index=s.index[::-1])

        s.plot(ax=ax)
        ax.set_ylabel('Hits')


class SitesAgentsGraph(object):
    """
    Attributes
    ----------
    project : str
        Either nowcoast or idpgis
    """

    def __init__(self, origin, project):
        """
        Parameters
        ----------
        project : str
            Either nowcoast or idpgis
        """
        self.origin = origin
        self.project = project

        self.wa = WebalizerAgents(self.origin, self.project)
        self.ws = WebalizerSites(self.origin, self.project)
        self.wt = WebalizerTraffic(self.origin, self.project)

    def run(self):

        fig, ax = plt.subplots(3, sharex=True, figsize=[18, 14])

        self.ws.run(ax[0])
        self.wa.run(ax[1])
        self.wt.run(ax[2])

        file = (f'/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common'
                f'/nowcoast/sites_agents/{self.origin}/{self.project}.png')

        plt.draw()
        fig.savefig(file)
