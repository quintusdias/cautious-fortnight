# Standard library imports
import datetime
import gzip
import itertools
import pathlib

# 3rd party library imports
from lxml import etree
import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
sns.set_style('darkgrid')
import yaml

# Local imports
from .converters import (
    convert_response, convert_bytes, convert_datatype, convert_bool,
    convert_timestamp
)


class Summarize(object):
    """
    Object for analyzing JMETER result files.

    Attributes
    ----------
    config : dict
        Configuration options loaded from YAML file.
    output_dir : path
        Write the HTML and associated output files here.
    df : pandas Dataframe
        Summarizes the output JMeter CSV files in long-form.
    index_tuples : list
        Tuples of run level and workspace:layer pairs.  This will form the
        index of the dataframe.
    gwc_data, gwc_index_tuples : lists
        GeoWebCache hit percentages from run level and workspace:layer pairs.
    gwc_df : dataframe
        Dataframe of geowebcache hit percentages.
    """
    def __init__(self, configfile, output_dir):
        """
        Parameters
        ----------
        config_file : path
            Raw CSV file
        output_dir : path
            Write the HTML and associated output files here.
        """
        # Read the load test configuration.
        self.configfile = configfile
        with open(self.configfile, mode='rt') as f:
            self.config = yaml.load(f)

        # As soon as we have the configuration file, we can determine what
        # the line colors and styles will be.
        colors = sns.color_palette(n_colors=7)

        linestyles = ['-', '--', ':', '-.']
        tuples = itertools.product(linestyles, colors)
        tuples = [
            item for idx, item in enumerate(tuples)
            if idx < len(self.config['testunits'])
        ]
        # tuples = tuples[:len(self.config['testunits'])]
        self.linestyles, self.colors = zip(*tuples)

        self.output_dir = pathlib.Path(output_dir)

        self.index_tuples = []
        self.data = []

        self.gwc_index_tuples = []
        self.gwc_data = []

    def run(self):
        """
        Analyze the Jmeter results files.
        """
        for run_level in range(len(self.config['intervals'])):
            for testunit in self.config['testunits']:
                self.process_service(run_level, testunit)

        # Standard JMeter CSV files.
        self.summarize_to_dataframe()
        self.summarize_response_headers()
        self.write_output()

    def write_output(self):
        """
        Write the raw data, HTML, plots, and tables describing the load test
        results.
        """
        self.output_dir.mkdir(exist_ok=True)

        self.write_raw_data_output()
        self.write_html_output()

    def write_raw_data_output(self):
        """
        Write the raw data.  Choose an output format, any output format, so
        long as it's HDF5.
        """
        path = self.output_dir / 'raw_data.h5'
        with pd.HDFStore(path) as store:
            store.put('geowebcache', self.gwc_df)
            store.put('jmeter', self.df)

    def add_link_to_raw_data(self, body):
        """
        Add a hyperlink to the raw data.
        """
        p = etree.SubElement(body, 'p')
        p.text = (
            'The tables from whence these plots were generated can be found '
        )
        a = etree.Element('a', href='raw_data.h5')
        a.text = 'here'
        a.tail = '.'
        p[:] = [a]

    def write_html_output(self):
        """
        Write the HTML, plots, and tables describing the load test results.
        """
        self.doc = etree.Element('html')

        header = etree.SubElement(self.doc, 'head')

        meta = etree.SubElement(header, 'meta')
        meta.attrib['http-equiv'] = 'refresh'
        meta.attrib['content'] = '60'

        body = etree.SubElement(self.doc, 'body')
        self.toc = etree.SubElement(body, 'ul', id='toc')

        self.add_link_to_raw_data(body)
        self._add_loadtest_configuration(body)
        self._generate_throughput_div(body)
        self._generate_bandwidth_div(body)
        self._generate_error_rate_div(body)
        self._generate_elapsed_div(body)
        self._generate_gwc_percentages(body)

        self.doc.getroottree().write(str(self.output_dir / 'index.html'),
                                     encoding='utf-8', pretty_print=True)

        # Save the dataframe as well.
        with pd.HDFStore(f"{self.output_dir / 'store.h5'}") as store:
            store['df'] = self.df

    def _add_loadtest_configuration(self, body):
        """
        Add description of load test configuration.
        """
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div',
                               id='configuration', name='configuration')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Configuration'
        pre = etree.SubElement(div, 'pre')
        with open(self.configfile, mode='rt') as f:
            pre.text = f.read()

        # Add it to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#configuration')
        a.text = 'Configuration'

    def _set_colors_and_linestyles(self, ax):
        for idx, line in enumerate(ax.lines):
            line.set_linestyle(self.linestyles[idx])
            line.set_color(self.colors[idx])

    def _generate_bandwidth_div(self, body):
        df = self.df['bytes'].unstack()
        self._reset_index_to_num_threads(df)

        kwargs = {
            'shortname': 'bytes',
            'ylabel': 'Bytes',
            'title': 'Average Number of Bytes Per Transaction',
        }
        self._generate_common_div(body, df, **kwargs)

    def _generate_common_div(self, body, df, shortname=None, ylabel=None,
                             title=None, text=None):
        """
        Create a plot and associated HTML elements.

        Parameters
        ----------
        body : lxml Element
            Corresponds to <BODY>
        df : pandas dataframe
            2D table with just a single metric of interest, spread out across
            all services.
        shortname : str
            Easy-to-remember name for the metric.
        ylabel, title : str
            Y-label on the plot and a title string.
        """
        # Create the plot
        fig, ax = plt.subplots()
        df.plot(ax=ax)
        self._set_colors_and_linestyles(ax)

        # Create a plot with the legend outside the box on the right (yes, the
        # right).  Make the extra items crowd the box as much as possible (i.e.
        # they are "tight").
        lgd = ax.legend(loc='center left', bbox_to_anchor=(1.05, 0.5))
        ax.set_ylabel(ylabel)
        path = self.output_dir / f"{shortname}.png"
        with path.open(mode='wb') as fp:
            fig.savefig(fp, bbox_extra_artists=(lgd,), bbox_inches='tight')

        # Link the plot into the document.  Put into it's own DIV with a title.
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div', id=shortname, name=shortname)
        h1 = etree.SubElement(div, 'h1')
        h1.text = title
        etree.SubElement(div, 'img', src=f"{shortname}.png")

        # Text is truly optional.
        if text is not None:
            p = etree.SubElement(div, 'p')
            p.text = text

        # Create an HTML table for the dataframe in question.  Don't print it
        # here, but use an external HTML file.
        table_filename = f"{shortname}.html"
        self.create_table_file(table_filename, df, title)

        # Create a link to the throughput table.
        p = etree.SubElement(div, 'p')
        a = etree.SubElement(p, 'a', href=table_filename)
        a.text = "table data"
        div.append(p)

        # Add this <DIV> into the table of contents at the top of the page.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href=f"#{shortname}")
        a.text = title

    def _generate_gwc_percentages(self, body):
        """
        Create a plot for the GeoWebCache percentages and write a nice table.
        """
        df = self.gwc_df['hit_pct'].unstack() 
        self._reset_index_to_num_threads(df)

        kwargs = {
            'shortname': 'hit_pct',
            'ylabel': 'GWC Hit Rate (%)',
            'title': 'GeoWebCache Hit Rate (only GWC headers)',
            'text': (
                "Calculated as the ratio of the number of responses that "
                "contain geowebcache-cache-result against the number of "
                "responses that contain geowebcache-cache-result with HIT."
            )
        }
        self._generate_common_div(body, df, **kwargs)

        df = self.gwc_df['hit_pct_no_empties'].unstack() 
        self._reset_index_to_num_threads(df)
        kwargs = {
            'shortname': 'hit_pct_no_empties',
            'ylabel': 'GWC Hit Rate (%)',
            'title': 'Non-empty GeoWebCache Hit Rate',
            'text': (
                "Calculated as the ratio of the number of all responses that "
                "that are not empty against the number of responses that "
                "contain geowebcache-cache-result with HIT."
            )
        }
        self._generate_common_div(body, df, **kwargs)

        df = self.gwc_df['hit_pct_all'].unstack() 
        self._reset_index_to_num_threads(df)
        kwargs = {
            'shortname': 'hit_pct_all',
            'ylabel': 'GWC Hit Rate (%)',
            'title': 'Overall GeoWebCache Hit Rate',
            'text': (
                "Calculated as the ratio of the number of all responses that "
                "against the number of responses (including empty responses) "
                "that contain geowebcache-cache-result with HIT."
            )
        }
        self._generate_common_div(body, df, **kwargs)

    def _generate_elapsed_div(self, body):
        """
        Create a plot for the elapsed time and write a nice table.
        """
        df = self.df['elapsed'].unstack() / 1000
        self._reset_index_to_num_threads(df)

        kwargs = {
            'shortname': 'elapsed',
            'ylabel': 'seconds',
            'title': 'Elapsed Time Per Transaction',
        }
        self._generate_common_div(body, df, **kwargs)

    def _generate_error_rate_div(self, body):
        """
        Create a plot for the throughput and write a nice table.
        """
        df = self.df['error_rate'].unstack()
        self._reset_index_to_num_threads(df)

        kwargs = {
            'shortname': 'error_rate',
            'ylabel': 'Error Rate (%)',
            'title': 'Error Rate',
        }
        self._generate_common_div(body, df, **kwargs)

    def _reset_index_to_num_threads(self, df):
        """
        Use the number of threads as the proxy for number of users.  Don't
        use run_level, which is what we have now.
        """
        new_idx = []
        for run_level in df.index:
            tot_num_threads = sum(item['num_threads'][run_level]
                                  for item in self.config['testunits'])
            new_idx.append(tot_num_threads)
        df.index = pd.Index(new_idx, name='NumThreads')

    def create_table_file(self, filename, df, caption):
        """
        Write an HTML file to summarize just a single metric.

        Parameters
        -----------
        filename : path or str
            HTML file presenting a single table.
        df : dataframe
            pandas dataframe describing a single metric, for all services, for
            all run levels
        caption : string
            Descriptive text
        """
        html = etree.Element('html')
        body = etree.SubElement(html, 'body')
        p = etree.SubElement(body, 'p')

        table_html_str = (df.T.style
                              .format("{:.1f}")
                              .set_caption(caption)
                              .render())
        table_doc = etree.HTML(table_html_str)
        table = table_doc.xpath('body/table')[0]
        p.append(table)

        path = self.output_dir / filename
        with pathlib.Path(path).open(mode='wb') as fp:
            html.getroottree().write(fp, encoding='utf-8', pretty_print=True)

    def _generate_throughput_div(self, body):
        """
        Create a plot for the throughput and write a nice table.
        """
        df = self.df['throughput'].unstack()
        self._reset_index_to_num_threads(df)

        max_rate = df.sum(axis=1).max()
        kwargs = {
            'shortname': 'overall_throughput',
            'ylabel': 'Overall Throughput (tr/sec)',
            'title': f'Overall Throughput: {max_rate:.0f} tr/sec',
        }
        self._generate_common_div(body, df.sum(axis=1).to_frame(), **kwargs)

        kwargs = {
            'shortname': 'by_service_throughput',
            'ylabel': 'By-Service Throughput (tr/sec)',
            'title': 'By-Service Throughput',
        }
        self._generate_common_div(body, df, **kwargs)

    def process_service(self, run_level, testunit):
        """
        Read the output of a test unit at a particular run level.  This
        constitutes a dataframe.
        """
        self.process_service_csv_results(run_level, testunit)
        self.process_service_response_headers(run_level, testunit)

    def process_service_response_headers(self, run_level, testunit):
        """
        Read the response headers of a test unit at a particular run level.
        """
        if not self.config['save_response_headers']:
            return

        workspace = testunit['name']

        root = pathlib.Path('.')
        path = root / f"{self.config['output_root']}" / f"{run_level:02d}"
        if (path / f"{workspace}.xml").exists():
            path = path / f"{workspace}.xml"
        else:
            path = path / f"{workspace}.xml.gz"
        print(f'Processing {path} ... ')

        # It is more than possible that the XML files are corrupt, so
        # we have to take extra precaution.
        parser = etree.XMLParser(recover=True)
        gf = gzip.GzipFile(path)
        tree = etree.parse(gf, parser=parser)

        # We count the total number of responses, the number of responses
        # that are empty (WTF are these?), the response count with gwc
        # information, and the number of gwc hits.
        response_count = 0
        empty_response_count = 0
        cache_count = 0
        cache_hit_count = 0

        for response in tree.xpath('/testResults/httpSample/responseHeader'):
            response_count += 1

            if response.text is None:
                # Curious, what is going on here?
                empty_response_count += 1
                continue

            for header in response.text.splitlines():
                if 'geowebcache-cache-result' in header:
                    cache_count += 1
                    if 'HIT' in header:
                        cache_hit_count += 1

        self.gwc_data.append({
            'response_count': response_count,
            'empty_response_count': empty_response_count,
            'cache_count': cache_count,
            'cache_hit_count': cache_hit_count,
        })

        self.gwc_index_tuples.append((run_level, workspace))

    def process_service_csv_results(self, run_level, testunit):
        """
        Read the output of a test unit at a particular run level.  This
        constitutes a dataframe.
        """
        workspace = testunit['name']

        root = pathlib.Path('.')
        path = root / f"{self.config['output_root']}" / f"{run_level:02d}"
        if (path / f"{workspace}.csv").exists():
            path = path / f"{workspace}.csv"
        else:
            path = path / f"{workspace}.csv.gz"
        print(f'Processing {path} ... ')

        kwargs = {
            'compression': 'infer',
            'converters': {
                'allThreads': convert_response,
                'bytes': convert_bytes,
                'dataType': convert_datatype,
                'grpThreads': convert_response,
                'responseCode': convert_response,
                'success': convert_bool,
                'timeStamp': convert_timestamp,
            },
            'index_col': 'timeStamp',
            'error_bad_lines': False,
            'warn_bad_lines': True,
        }
        df = pd.read_csv(path, **kwargs)

        # By summing the success field, we get the throughput by dividing the
        # number of successes by the interval.
        s = df.sum(numeric_only=True)

        # if s['success'] != s['dataType']:
        #     msg = 'bad news'
        #     raise RuntimeError(msg)

        # s['throughput'] = s['success']
        s['throughput'] = s['dataType']
        s['throughput'] /= (self.config['intervals'][run_level] * 60)

        s['ntrans'] = df.shape[0]
        self.data.append(s.values)
        self.index_tuples.append((run_level, f"{workspace}"))

    def summarize_response_headers(self):
        """
        The response data has been collected as 4-tuples of:
        
            #1 the number of responses recorded
            #2 the number of empty responses, with NO data what-so-ever
            #3 the number of responses with the geowebcache-cache-result header
            #4 the number of geowebcache-cache-result responses with "HIT"

        The hit rate is calculated as the ratio of HITs against increasingly
        hostile metrics.
        """
        # All the data is collected.
        index = pd.MultiIndex.from_tuples(self.index_tuples,
                                          names=['run_level', 'service'])
        df = pd.DataFrame(self.gwc_data, index=index)

        # Only use responses where gwc information is present.
        df['hit_pct'] = df['cache_hit_count'] / df['cache_count'] * 100

        # Use all responses, including those where there are no GWC headers.
        # This includes empty responses.
        df['hit_pct_all'] = df['cache_hit_count'] / df['response_count'] * 100

        # Use all responses with GWC headers, plus those responses that are
        # NOT empty.
        #
        # HIT_PCT_ALL <= HIT_PCT_NO_EMPTIES <= HIT_PCT
        no_empties = df['response_count'] - df['empty_response_count']
        df['hit_pct_no_empties'] = df['cache_hit_count'] / no_empties * 100

        self.gwc_df = df

    def summarize_to_dataframe(self):
        """
        Aggregate all data into a multi-index dataframe.
        """
        # All the data is collected.
        index = pd.MultiIndex.from_tuples(self.index_tuples,
                                          names=['run_level', 'service'])
        columns = [
            'elapsed', 'responseCode', 'dataType', 'success', 'bytes',
            'grpThreads', 'allThreads', 'latency', 'throughput', 'ntrans'
        ]
        df = pd.DataFrame(self.data, index=index, columns=columns)

        # These are just averages.
        df['bytes'] /= df['ntrans']
        df['elapsed'] /= df['ntrans']
        df['latency'] /= df['ntrans']

        # Yes, dataType is a better measure of success than success.
        df['error_rate'] = (1 - df['dataType'] / df['ntrans']) * 100

        # Only keep what we need.
        keepers = [
            'elapsed', 'throughput', 'success', 'bytes', 'latency', 'ntrans',
            'error_rate'
        ]
        self.df = df[keepers]
