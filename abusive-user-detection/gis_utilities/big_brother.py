# Standard library imports
import datetime as dt
from urllib.parse import urlparse

# 3rd party library imports
import requests
from lxml import etree
import pandas as pd

_NOWCOAST_OPS_CPRK = 'http://bb.ncep.noaa.gov/IDP-Applications/IDP-NOWCOAST-NEW-Ops/IDP-NOWCOAST-NEW-Ops.html'

class BBFlagHistory(object):

    def __init__(self, url, output_file=None):
        """
        Parameters
        ----------
        url : str
            Top-level Big Brother URL.
        scheme, netloc : str
            Portions of the URL, i.e.
            <scheme>://<netloc>/<path>;<params>?<query>#<fragment>
        output : str or None
            File or filename to which output is written.
        """
        self.url = url
        self.output_file = output_file

        o = urlparse(self.url)
        self.scheme = o.scheme
        self.netloc = o.netloc

        r = requests.get(self.url)
        self.doc = etree.HTML(r.content.decode('utf-8'))

        self.table = self.doc.xpath('//table[@summary="Group Block"]')[0]

        # Get the names of the columns.
        self.columns = self.table.xpath('//a/font[@color="teal"]/b/text()')
        print(self.columns)

        # Get the names of the hosts.
        path = 'tr/td[@nowrap]/a[2]/font/text()'
        hosts = self.table.xpath(path)
        if len(hosts) == 0:
            # new nowcoast cprk on op?
            path = 'tr/td[@nowrap]/a[1]/font/text()'
            hosts = self.table.xpath(path)
        self.hosts = hosts

        print(self.hosts)

        self.setup_output_document()

    def setup_output_document(self):
        """
        Create the output HTML document if specified.
        """

        if self.output_file is None:
            self.output_doc = None
            return

        self.output_doc = etree.Element('html')

        head = etree.SubElement(self.output_doc, 'head')
        link = etree.SubElement(head, 'link')
        link.attrib['rel'] = 'stylesheet'
        link.attrib['type'] = 'text/css'
        link.attrib['href'] = '../summary.css'

        body = etree.SubElement(self.output_doc, 'body')

        # Add the current date so we know when it last updated.
        p = etree.SubElement(body, 'p')
        p.attrib['style'] = 'text-align:right'
        p.text = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        otable = etree.SubElement(body, 'table')

        # Add a header row.
        tr = etree.SubElement(otable, 'tr')
        tr.attrib['id'] = 'header'

        # Add an empty TH to allow for the row headers.
        etree.SubElement(tr, 'th')

        for column in self.columns:
            th = etree.SubElement(tr, 'th')
            th.text = column

        for host in self.hosts:
            tr = etree.SubElement(otable, 'tr')
            tr.attrib['id'] = host

            # Host header
            th = etree.SubElement(tr, 'th')
            th.text = host.split('.')[0]

            for column in self.columns:
                td = etree.SubElement(tr, 'td')
                td.attrib['id'] = column

    def run(self):
        """
        Interrogate BB lead-in screen.
        """

        for column in self.columns:
            # Go thru each BB column
            if self.output_file is None:
                print(column)
            for host in self.hosts:
                # Look for a hyperlink specific to both the host and the BB
                # column.  If we find one, then that combination needs to be
                # interrogated.
                if self.url == _NOWCOAST_OPS_CPRK:
                    # path = f'.//tr[3]/td/a[@href="/bb/html/{host}.{column}.html"]'
                    path = f'//a[@href="/html/{host}.{column}.html"]'
                else:
                    path = f'//a[@href="/html/{host}.{column}.html"]'
                elts = self.table.xpath(path)

                if len(elts) > 0:
                    self.process_host_column(host, column)

        if self.output_file is not None:
            with open(self.output_file, mode='wb') as f:
                f.write(etree.tostring(self.output_doc))

    def process_host_column(self, host, column):
        """
        Interrogate the history screen for the host and the column.
        """
        print(host, column)
        url = self.scheme + '://' + self.netloc + '/cgi-bin/bb-hist.sh'
        params = {
            'HISTFILE': host.replace('.', ',') + '.' + column,
            'ENTRIES': 50,
        }

        r = requests.get(url, params=params)
        doc = etree.HTML(r.content.decode('utf-8'))

        # Get the table with the detailed flag history.
        tables = doc.xpath('//table')
        table = tables[5]
        percentages = table.xpath('tr[3]/td/b/text()')
        percentages = [x.replace('%', '') for x in percentages[:4]]
        G, Y, R, P = percentages

        if self.output_file is None:
            fmt = "    {host:40}: {green} {yellow} {red} {purple}"
            text = fmt.format(host=host, green=G, yellow=Y, red=R, purple=P)
            print(text)
        else:
            # Locate the TD element for the host and column.  If the flag was
            # green 100% of the time, supply the green flag gif.  Otherwise
            # fill in the numbers.
            path = './/tr[@id="{host}"]/td[@id="{column}"]'
            path = path.format(host=host, column=column)
            td = self.output_doc.xpath(path)[0]

            if G == '100':
                # Just supply a green gif.  We were green 100% of the time.
                img = etree.SubElement(td, 'img')
                img.attrib['src'] = '/ncep_common/nowcoast/images/green.gif'
            else:
                # Fill in the actual numbers and color them to make them stand
                # out.
                bb_colors = ['green', 'yellow', 'red', 'purple']
                for percentage, color in zip(percentages, bb_colors):
                    if color == 'green':
                        continue
                    if percentage == '0':
                        continue
                    span = etree.SubElement(td, 'span')
                    span.attrib['style'] = 'color:' + color
                    span.text = ' ' + percentage + ' '
