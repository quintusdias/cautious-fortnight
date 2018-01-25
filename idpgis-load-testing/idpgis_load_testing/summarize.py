# Standard library imports
import datetime
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


def convert_bytes(r):
    """
    Convert bytes column.

    JMETER usually provides a "bytes" field which gives the length of
    the payload delivered by the HTTP request.

    The problem comes when HTTP requests fail, as the column values
    can get interchanged.  When that happens, the resulting value may
    not be convertible to integer.  By using this converter function,
    we can catch such instances and just give them a -1 value.

    Parameters
    ----------
    r : str
        Value from bytes column of JMETER results file.

    Returns
    -------
    bool
        Either True or False, the success or failure of the HTTP request.
    """
    try:
        return int(r)
    except ValueError:
        return -1


def convert_success(item):
    """
    Convert success field.

    JMETER usually provides a "success" field which is either "true"
    or "false".  By specifying it as boolean, it can automatically be
    converted into True or False.

    The problem comes when HTTP requests fail, as the column values can
    get interchanged.  When that happens, the resulting value is often
    "text", which classifies as neither True nor False.  By using this
    converter function, we can catch such instances and classify them
    as False.

    Parameters
    ----------
    item : str
        Value from success column of JMETER results file.

    Returns
    -------
    bool
        Either True or False, the success or failure of the HTTP request.
    """
    if item == 'true':
        return True
    else:
        return False


def convert_response(r):
    """
    Provide integer reponse code.

    The responseCode value in a JMETER log file should be the typical
    HTTP 200 value.

    Sometimes, though, it is not.  It seems that when JMETER encounters
    a bad request, instead of a 500 or 404 or what-have-you, the
    reponse code column value is "Export map image with bounding box".
    This causes pandas to complain about mixed datatype columns, which
    is memory-intensive and slows things down.  Since all we really
    care about is success or failure, we will catch such exceptional
    rows and return -1 instead.

    Parameters
    ----------
    str
        Value from responseCode column of JMETER results file, usually
        '200'.

    Returns
    -------
    int
        Either 200 for success or -1 for a fail.
    """
    try:
        return int(r)
    except (TypeError, ValueError):
        return -1


def convert_timestamp(t):
    """
    Convert from milliseconds after the epoch to native datetime.

    Parameters
    ----------
    t : str
        Milliseconds after the epoch.

    Returns
    -------
    datetime.datetime
        Standard python datetime value.
    """
    try:
        ts = datetime.datetime.utcfromtimestamp(float(t) / 1000.0)
    except (OSError, ValueError, OverflowError) as e:
        print(repr(e))
        # Saw this once with a mangled line.
        #
        # 1508256351508273690379,1220,EXPORT,200,OK,
        # export wwa_meteoceanhydro_shortduration_hazards_warnings_time 8-1,
        # bin,true,3242,1,23,233
        #
        return None
    if ts.year > 2100:
        # Have seen one instance where a timestamp got corrupted.
        return None
    else:
        return ts


def convert_bool(t):
    try:
        return bool(t)
    except Exception as e:
        return False


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

    def write_output(self):
        """
        Write the HTML, plots, and tables describing the load test results.
        """
        self.output_dir.mkdir(exist_ok=True)

        self.doc = etree.Element('html')

        header = etree.SubElement(self.doc, 'head')
        style = etree.SubElement(header, 'style', type='text/css')

        # Write the global table styles.  We do this here instead of using
        # the dataframe option because we only need do it once.
        style.text = (
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

        body = etree.SubElement(self.doc, 'body')
        self.toc = etree.SubElement(body, 'ul', id='toc')

        self._add_loadtest_configuration(body)
        self._generate_throughput_div(body)
        self._generate_bandwidth_div(body)
        self._generate_error_rate_div(body)
        self._generate_elapsed_div(body)

        self.doc.getroottree().write(str(self.output_dir / 'index.html'),
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

    def _generate_bandwidth_div(self, body):
        """
        Create a plot for the bandwidth and write a nice table.
        """
        df = self.df['bytes'].unstack()

        # Create the plot
        fig, ax = plt.subplots()
        df.plot(ax=ax)
        ax.set_xticks(list(range(len(self.config['intervals']))))
        ax.set_ylabel('Bytes')
        output_file = str(self.output_dir / 'bytes.png')
        fig.savefig(output_file)

        # Create the HTML for the table.
        table_html_str = (df.style
                            .format("{:.0f}")
                            .set_caption("Bytes")
                            .render())
        table_doc = etree.HTML(table_html_str)
        table = table_doc.xpath('body/table')[0]

        # Link the plot and table into the document.
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div', id='bytes', name='bytes')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Average Number of Bytes Per Run Level'
        etree.SubElement(div, 'img', src="bytes.png")
        div.append(table)

        # Add the throughput section to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#bytes')
        a.text = 'Bytes'

    def _generate_elapsed_div(self, body):
        """
        Create a plot for the elapsed time and write a nice table.
        """
        df = self.df['elapsed'].unstack() / 1000

        # Create the plot
        fig, ax = plt.subplots()
        df.plot(ax=ax)
        ax.set_xticks(list(range(len(self.config['intervals']))))
        ax.set_ylabel('seconds')
        output_file = str(self.output_dir / 'elapsed.png')
        fig.savefig(output_file)

        # Create the HTML for the table.
        table_html_str = (df.style
                            .format("{:.1f}")
                            .set_caption("Elapsed")
                            .render())
        table_doc = etree.HTML(table_html_str)
        table = table_doc.xpath('body/table')[0]

        # Link the plot and table into the document.
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div', id='elapsed', name='elapsed')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Elapsed Time Per Transaction'
        etree.SubElement(div, 'img', src="elapsed.png")
        div.append(table)

        # Add the throughput section to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#elapsed')
        a.text = 'Elapsed time per transaction'

    def _generate_error_rate_div(self, body):
        """
        Create a plot for the throughput and write a nice table.
        """
        df = self.df['error_rate'].unstack() / 1000

        # Create the plot
        fig, ax = plt.subplots()
        df.plot(ax=ax)
        ax.set_xticks(list(range(len(self.config['intervals']))))
        ax.set_ylabel('%')
        output_file = str(self.output_dir / 'error_rate.png')
        fig.savefig(output_file)

        # Create the HTML for the table.
        table_html_str = (df.style
                            .format("{:.1f}")
                            .set_caption("Error Rate")
                            .render())
        table_doc = etree.HTML(table_html_str)
        table = table_doc.xpath('body/table')[0]

        # Link the plot and table into the document.
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div', id='error_rate', name='error_rate')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Error Rate'
        etree.SubElement(div, 'img', src="error_rate.png")
        div.append(table)

        # Add the throughput section to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#error_rate')
        a.text = 'Error rate'

    def _generate_throughput_div(self, body):
        """
        Create a plot for the throughput and write a nice table.
        """
        df = self.df['throughput'].unstack()

        # Create the plot
        fig, ax = plt.subplots()
        df.plot(ax=ax)
        ax.set_xticks(list(range(len(self.config['intervals']))))
        ax.set_ylabel('tr/sec')
        output_file = str(self.output_dir / 'throughput.png')
        fig.savefig(output_file)

        # Create the HTML for the table.
        table_html_str = (df.style
                            .format("{:.1f}")
                            .set_caption("Throughput")
                            .render())
        table_doc = etree.HTML(table_html_str)
        table = table_doc.xpath('body/table')[0]

        # Link the plot and table into the document.
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div', id='throughput', name='throughput')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Throughput'
        etree.SubElement(div, 'img', src="throughput.png")
        div.append(table)

        # Add the throughput section to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#throughput')
        a.text = 'Throughput'

    def process_service(self, run_level, testunit):

        folder, service, service_type = testunit['service'].split('/')

        root = pathlib.Path('.')
        path = root / f"{self.config['output_root']}" / f"{run_level:02d}"
        path = path / folder / service / f"{service_type}.csv"
        print(f'Processing {path} ... ')

        kwargs = {
            'converters': {
                'allThreads': convert_response,
                'bytes': convert_bytes,
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

        s = df.sum(numeric_only=True)

        s['throughput'] = s['success']
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
        columns = ['elapsed', 'responseCode', 'success', 'bytes', 'grpThreads',
                   'allThreads', 'latency', 'throughput', 'ntrans']
        df = pd.DataFrame(self.data, index=index, columns=columns)

        # These are just averages.
        df['bytes'] /= df['ntrans']
        df['elapsed'] /= df['ntrans']
        df['latency'] /= df['ntrans']
        df['error_rate'] = (1 - df['success'] / df['ntrans']) * 100

        # Only keep what we need.
        keepers = [
            'elapsed', 'throughput', 'success', 'bytes', 'latency', 'ntrans',
            'error_rate'
        ]
        self.df = df[keepers]
