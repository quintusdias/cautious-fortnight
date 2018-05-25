# Standard library imports
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
    convert_bytes, convert_response, convert_bool, convert_timestamp,
    convert_datatype
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
        # colors = mpl.rcParams['axes.prop_cycle'].by_key()['color']
        colors = sns.color_palette(n_colors=7)

        linestyles = ['-', '--', ':', '-.']
        tuples = list(itertools.product(linestyles, colors))
        tuples = tuples[:len(self.config['testunits'])]
        self.linestyles, self.colors = zip(*tuples)

        self.output_dir = pathlib.Path(output_dir)

        self.index_tuples = []
        self.data = []

    def run(self):
        """
        Analyze the Jmeter results files.
        """
        for run_level in range(len(self.config['intervals'])):
            for testunit in self.config['testunits']:
                self.process_service(run_level, testunit)

        self.summarize_to_dataframe()
        self.write_output()

    def write_css(self):
        """
        Write the CSS for the summarizing HTML.
        """
        # Write the global table styles.  We do this here instead of using
        # the dataframe option because we only need do it once.
        text = (
            "\n"
            "table {\n"
            "    border-collapse: collapse;\n"
            "}\n"
            "table td {\n"
            "    border-right: 1px solid #99CCCC;\n"
            "    border-bottom:  1px solid #99CCCC;\n"
            "    text-align:  right;\n"
            "}\n"
            "table th {\n"
            "    border-right: 3px solid #99CCCC;\n"
            "    border-bottom:  1px solid #99CCCC;\n"
            "    padding-right:  .3em;\n"
            "    text-align:  left;\n"
            "}\n"
            "table th[scope=\"col\"] {\n"
            "    border-right: 1px solid #99CCCC;\n"
            "    border-bottom:  3px solid #99CCCC;\n"
            "    padding-right:  .3em;\n"
            "}\n"
            "table th[scope=\"col\"]:first-child {\n"
            "    border-right: 3px solid #99CCCC;\n"
            "}\n"
            "table th[scope=\"col\"]:last-child {\n"
            "    border-right: 0;\n"
            "}\n"
            "table tr:last-child th {\n"
            "    border-bottom: 0;\n"
            "}\n"
        )

        path = self.output_dir / 'styles.css'
        with path.open(mode='wt') as f:
            f.write(text)


    def write_output(self):
        """
        Write the HTML, plots, and tables describing the load test results.
        """
        self.output_dir.mkdir(exist_ok=True)

        self.write_css()

        self.doc = etree.Element('html')

        header = etree.SubElement(self.doc, 'head')
        etree.SubElement(header, 'link', rel='stylesheet', href='styles.css')

        body = etree.SubElement(self.doc, 'body')
        self.toc = etree.SubElement(body, 'ul', id='toc')

        # Add section for the user to later describe the load test
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div', id="description")
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Synopsis'

        self._add_loadtest_configuration(body)
        self._generate_throughput_div(body)
        self._generate_bandwidth_div(body)
        self._generate_error_rate_div(body)
        self._generate_elapsed_div(body)

        # Add a final TOC entry for CheckMK.  We assume that this is
        # populated later.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='checkmk')
        a.text = 'CheckMK'

        output_file = str(self.output_dir / 'index.html')
        self.doc.getroottree().write(output_file,
                                     encoding='utf-8', pretty_print=True)

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

    def _set_colors_and_linestyles(self, ax):
        for idx, line in enumerate(ax.lines):
            line.set_linestyle(self.linestyles[idx])
            line.set_color(self.colors[idx])

    def _generate_bandwidth_div(self, body):
        """
        Create a plot for the bandwidth and write a nice table.
        """
        df = self.df['bytes'].unstack()
        self._reset_index_to_num_threads(df)

        self._simplify_labels(df)
        kwargs = {
            'shortname': 'bytes',
            'ylabel': 'Bytes',
            'title': 'Average Number of Bytes Per Transaction',
        }
        self._generate_common_div(body, df, **kwargs)

    def _generate_elapsed_div(self, body):
        """
        Create a plot for the elapsed time and write a nice table.
        """
        df = self.df['elapsed'].unstack() / 1000
        self._reset_index_to_num_threads(df)
        self._simplify_labels(df)

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
        self._simplify_labels(df)

        kwargs = {
            'shortname': 'error_rate',
            'ylabel': 'Error Rate (%)',
            'title': 'Error Rate',
        }
        self._generate_common_div(body, df, **kwargs)

    def _simplify_labels(self, df):
        """
        The dataframe row labels come in looking something like

        "folder/services/{MapServer,ImageServer,FeatureServer}"

        Discard the folder name and discard the string "MapServer"
        if present, because most of the services are MapServers.
        """
        lst = []
        for label in df.columns:
            # Don't bother saying it's a MapServer
            label = label.replace('/MapServer', '')

            # Don't bother with the folder.
            label = '/'.join(label.split('/')[1:])
            lst.append(label)
        df.columns = lst

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

        self._simplify_labels(df)
        kwargs = {
            'shortname': 'by_service_throughput',
            'ylabel': 'By-Service Throughput (tr/sec)',
            'title': 'By-Service Throughput',
        }
        self._generate_common_div(body, df, **kwargs)

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

    def process_service(self, run_level, testunit):

        folder, service, service_type = testunit['service'].split('/')

        root = pathlib.Path('.')
        root = root / f"{self.config['output_root']}" / f"{run_level:02d}"
        path = root / folder / service / f"{service_type}.csv"

        kwargs = {
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

        if not path.exists():
            path = root / folder / service / f"{service_type}.csv.gz"
            if path.exists():
                kwargs['compression'] = 'gzip'
            else:
                raise RuntimeError(f'Could not find {path}.')

        print(f'Processing {path} ... ')
        df = pd.read_csv(path, **kwargs)

        s = df.sum(numeric_only=True)

        s['throughput'] = s['dataType']
        s['throughput'] /= (self.config['intervals'][run_level] * 60)

        s['ntrans'] = df.shape[0]
        self.data.append(s.values)
        self.index_tuples.append((run_level, f"{testunit['service']}"))

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
        df['error_rate'] = (1 - df['dataType'] / df['ntrans']) * 100

        # Only keep what we need.
        keepers = [
            'elapsed', 'throughput', 'success', 'bytes', 'latency', 'ntrans',
            'error_rate'
        ]
        self.df = df[keepers]
