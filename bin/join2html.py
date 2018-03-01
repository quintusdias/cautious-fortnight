#!/usr/bin/env python3

# standard libraries
import sys
from functools import reduce

# Third party libraries
import pandas as pd

def join_dataframes():
    """
    """
    columns = ['service', 'hits', 'bandwidth']
    df = pd.read_csv('p_all.dat', sep='\s+', header=None,
                     names=columns, index_col='service')
    df_all = df

    columns = ['service', 'geoevent hits', 'geoevent bandwidth']
    df = pd.read_csv('p_geoevent.dat', sep='\s+', header=None,
                     names=columns, index_col='service')
    df_geoevent = df

    columns = ['service', 'wms mapdraws', 'wms bandwidth']
    df = pd.read_csv('p_wms.dat', sep='\s+', header=None,
                     names=columns, index_col='service')
    df_wms = df

    columns = ['service', 'export mapdraws', 'export bandwidth']
    df = pd.read_csv('p_export.dat', sep='\s+', header=None,
                     names=columns, index_col='service')
    df_export = df

    columns = ['service', 'wmts mapdraws', 'wmts bandwidth']
    df = pd.read_csv('p_wmts.dat', sep='\s+', header=None,
                     names=columns, index_col='service')
    df_wmts = df

    columns = ['service', 'wfs getfeature', 'wfs getfeature bandwidth']
    df = pd.read_csv('p_wfs_getfeature.dat', sep='\s+', header=None,
                     names=columns, index_col='service')
    df_wfs_getfeature = df

    columns = ['service', 'code', 'hits']
    df = pd.read_csv('p_errors.dat', sep='\s+', header=None, names=columns)
    df_errors = df.pivot(index='service', columns='code', values='hits')

    dfs = [
        df_all, df_geoevent, df_wms, df_export, df_wmts, df_wfs_getfeature,
        df_errors
    ]
    reducer = lambda left, right: pd.merge(left, right, left_index=True,
                                           right_index=True, how='outer')
    df = reduce(reducer, dfs)

    # NaNs really should be zeros.
    df.fillna(value=0, inplace=True)
    df.sort_values(by='hits', ascending=False, inplace=True)

    # Sort according to hits
    df.to_csv('hits.csv')

if __name__ == "__main__":
    join_dataframes()
