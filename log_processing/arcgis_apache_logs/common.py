# Standard library imports
import logging
import pathlib
import sqlite3

# 3rd party library imports
from lxml import etree
import matplotlib.pyplot as plt
import pandas as pd


def millions_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.2f}M'


def thousands_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e3):.3f}K'


class CommonProcessor(object):
    """
    Attributes
    ----------
    conn : obj
        database connectivity
    database : path or str
        Path to database
    frequency : str
        How to resample the dataframe of apache log records.
    project : str
        Either nowcoast or idpgis
    records : list
        Raw records collected, one for each apache log entry.
    """
    def __init__(self, project, document_root=None, logger=None):

        self.project = project

        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)

        if document_root is None:
            self.root = pathlib.Path.home() \
                        / 'Documents' \
                        / 'arcgis_apache_logs'
        else:
            self.root = pathlib.Path(document_root)

        if not self.root.exists():
            self.root.mkdir(parents=True, exist_ok=True)

        self.database = self.root / f'arcgis_apache_{self.project}.db'
        self.conn = sqlite3.connect(self.database)
        self.cursor = self.conn.cursor()

        # Force foreign key support.
        self.conn.execute("PRAGMA foreign_keys = 1")

        self.verify_database_setup()

        self.MAX_RAW_RECORDS = 100000000

        self.records = []
        self.frequency = '1H'

    def extract_html_table_from_dataframe(self, df):
        """
        Create an HTML <TABLE> from a dataframe.

        Parameters
        ----------
        df : dataframe
            Hits, bytes, and errors for apache logs

        Returns
        -------
            lxml.etree Element describing an HTML <TABLE>
            CSS decribing the table
        """
        table_styles = [
            # This one doesn't look like it's needed.
            dict(selector='table', props=[('border-collapse', 'collapse')]),
            # Each column header has a solid bottom border and some padding.
            dict(selector='th', props=[('border-bottom', '2px solid #069'),
                                       ('padding', '5px 3px')]),
            # Each cell has a less solid bottom border and the same padding.
            dict(selector='td', props=[('text-align', 'right'),
                                       ('border-bottom', '1px solid #069'),
                                       ('padding', '5px 3px')]),
        ]

        format = {
            'hits': '{:,.0f}',
            'hits %': '{:.1f}',
            'mapdraw %': '{:.1f}',
            'GBytes': '{:,.1f}',
            'GBytes %': '{:.1f}',
            'errors': '{:,.0f}',
            'errors: % of all hits': '{:,.1f}',
            'errors: % of all errors': '{:,.1f}',
        }
        tablestr = (df.style
                      .set_table_styles(table_styles)
                      .format(format)
                      .render())

        tree_doc = etree.HTML(tablestr)

        table = tree_doc.xpath('body/table')[0]

        table_css = tree_doc.xpath('head/style')[0].text
        return table, table_css

    def get_timeseries(self):
        """
        Collect a timeseries of information from the "*_logs" table.  The
        data should be summed/aggregated for each time interval.
        """

        df = pd.read_sql(self.time_series_sql, self.conn)

        # Right now the 'date' column is in timestamp form.  We need that
        # in native datetime.
        df['date'] = pd.to_datetime(df['date'], unit='s')

        self.df = df
        self.df_today = self.df[self.df.date.dt.day == self.df.date.max().day]

    def write_html_and_image_output(self, df, html_doc, title=None,
                                    filename=None, yaxis_formatter=None,
                                    folder=None, text=None):

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        if yaxis_formatter is not None:
            ax.yaxis.set_major_formatter(yaxis_formatter)

        ax.set_title(title)

        # Shrink the axis to put the legend outside.
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])

        handles, labels = ax.get_legend_handles_labels()

        # Restrict the legend to just the top seven labels.
        handles = handles[:7]
        labels = labels[:7]
        ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))

        path = self.root / filename
        plt.savefig(path)
        plt.close(fig)

        # Create the HTML for the image.
        body = html_doc.xpath('body')[0]
        div = etree.SubElement(body, 'div')

        if folder is not None:

            a = etree.SubElement(div, 'a', name=folder)
            h2 = etree.SubElement(div, 'h2')
            h2.text = folder

            if text is not None:
                p = etree.SubElement(div, 'p')
                p.text = text

            etree.SubElement(div, 'img', src=filename)

            # Link us in to the table of contents.
            ul = body.xpath('.//ul[@id="services"]')[0]
            li = etree.SubElement(ul, 'li')
            a = etree.SubElement(li, 'a', href=f'#{folder}')
            a.text = folder

        else:

            if text is not None:
                p = etree.SubElement(div, 'p')
                p.text = text

            a = etree.SubElement(div, 'a')
            etree.SubElement(div, 'img', src=filename)

    def create_html_table(self, df, html_doc, atext=None, aname=None,
                          h1text=None, ptext=None):
        """
        Create a <TABLE> from the dataframe.
        """

        table, css = self.extract_html_table_from_dataframe(df)

        # extract the CSS and place into our own document.
        style = html_doc.xpath('head/style')[0]
        style.text = style.text + '\n' + css

        body = html_doc.xpath('body')[0]
        div = etree.SubElement(body, 'div')
        etree.SubElement(div, 'hr')
        a = etree.SubElement(div, 'a', name=aname)
        h1 = etree.SubElement(div, 'h1')
        h1.text = h1text

        if ptext is not None:
            p = etree.SubElement(div, 'p')
            p.text = ptext

        # Add to the table of contents.
        toc = html_doc.xpath('body/ul[@class="tableofcontents"]')[0]
        li = etree.SubElement(toc, 'li')
        a = etree.SubElement(li, 'a', href=f'#{aname}')
        a.text = atext

        div.append(table)

    def merge_with_database(self, df_current, table):
        """
        The current set of records may overlap with existing records in the
        database, so we must merge them.
        """
        start = df_current.iloc[0].date

        # Get everything from the database after this time.
        sql = f"""
               SELECT *
               FROM {table}
               WHERE date >= ?
               ORDER BY date
               """
        df_database = pd.read_sql(sql, self.conn, params=(start,))
        if df_database.shape[0] == 0:
            # Nothing to merge.
            return df_current

        # Delete those rows, as we will be replacing them.
        sql = f"""
               DELETE
               FROM {table}
               WHERE date >= ?
               """
        self.cursor.execute(sql, (start,))

        # Aggregate the two dataframes together.
        if table == 'summary':
            group_cols = ['date']
        else:
            group_cols = ['date', 'id']
        df = (pd.concat((df_current, df_database), axis='index', sort=False)
                .groupby(group_cols)
                .sum()
                .reset_index())
        return df
