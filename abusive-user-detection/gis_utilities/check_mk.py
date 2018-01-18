# Standard library imports
import datetime as dt
import itertools
import pathlib
import re
import warnings

# 3rd party library imports
from lxml import etree
import lxml.html
import requests
import yaml


class CheckCheckMK(object):
    """
    Attributes
    ----------
    parameter : str
        Get plots from Check_MK for this parameter.
    machines : list
        Get plots for each machine in this list.
    s : requests.Session
        Query using this object.
    timerange : 2-tuple
        Start and stop time for queries.
    """
    def __init__(self, machines, parameter, timerange, output_root):

        if not type(machines) == list:
            machines = list(machines)
        self.machines = self.expand_machines(machines)
        self.read_config()

        self.parameter = parameter
        self.timerange = tuple(timerange)

        self.output_root = pathlib.Path(output_root)
        self.output_root.mkdir(parents=True, exist_ok=True)

        self.s = requests.Session()
        self.s.verify = False

        self.log_into_check_mk()

    def read_config(self):
        """
        Read the user's checkmk credentials.  Stored as a YAML file in
        $HOME/.config/ccmk/config.yaml.
        """
        path = pathlib.Path.home() / '.config' / 'ccmk' / 'config.yaml'
        if not path.exists():
            msg = (f"The configuration file storing your checkmk credentials "
                   f"- {path} - does not exist.  Please create it.")
            raise RuntimeError(msg)
        with path.open(mode='rt') as f:
            self.config = yaml.load(f)

    def expand_machines(self, machines_in):

        # Expand the machines
        machines = []
        for machine in machines_in:
            print(machine)

            regex = re.compile('[[][\w]+[]]')
            parts = [m for m in regex.finditer(machine)]
            if len(parts) == 0:
                # No expandable regex found, it must be a single machine name.
                machines.append(machine)
                continue

            # This is the expandable part to be blown up by itertools.
            iterables = [m.group()[1:-1] for m in parts]

            # construct the full list.
            pattern = '(?P<n0>[\w.-]*)'
            for idx, iterable in enumerate(iterables):
                pattern += f'(?P<i{idx + 1}>[[]{iterable}[]])'
                pattern += f'(?P<n{idx + 1}>[\w.-]*)'

            regex = re.compile(pattern)
            m = regex.match(machine)

            args = []
            for part in m.groups():
                if part == '':
                    pass
                elif part[0] == '[':
                    # an iterable:
                    args.append(part[1:-1])
                else:
                    # Must make this part an iterable.
                    args.append([part])
            args = tuple(args)

            tuples = list(itertools.product(*args))
            machines.extend([''.join(t) for t in tuples])

        return machines

    def log_into_check_mk(self):

        with warnings.catch_warnings():
            # Ignore warning about SSL certificates
            warnings.simplefilter('ignore')

            url = 'https://vm-lnx-checkmk.ncep.noaa.gov/ncep/check_mk/login.py'

            login = self.s.get(url)
            login_html = lxml.html.fromstring(login.text)
            hidden_inputs = login_html.xpath(r'//form//input[@type="hidden"]')
            form = {x.attrib['name']: x.attrib['value'] for x in hidden_inputs}
            form['_username'] = self.config['username']
            form['_password'] = self.config['password']

            self.s.post(url, data=form)

    def run(self):

        images = self.query_check_mk()

        doc = etree.Element('html')
        body = etree.SubElement(doc, 'body')

        div = etree.SubElement(body, 'div')
        p = etree.SubElement(div, 'p')
        p.text = f"Last query at {dt.datetime.now().strftime('%c')}"

        div = etree.SubElement(body, 'div')
        for image, machine in zip(images, self.machines):
            image_name = f"{machine}.png"

            path = self.output_root / image_name
            with path.open(mode='wb') as f:
                f.write(image)

            img_elt = etree.SubElement(div, 'img')
            img_elt.attrib['src'] = image_name

        path = self.output_root / 'index.html'
        etree.ElementTree(doc).write(str(path), pretty_print=True)

    def query_check_mk(self):

        url = (
            'https://vm-lnx-checkmk.ncep.noaa.gov'
            '/ncep/pnp4nagios/index.php/image'
        )

        try:
            parameter, source = self.parameter.split(':')
        except ValueError:
            # If no ":" is there, then the source is 0 and the parameter is
            # what was originally specified.
            source = 0
            parameter = self.parameter
        else:
            source = int(source)

        params = {
            'baseurl': 'https://vm-lnx-checkmk.ncep.noaa.gov/ncep/check_mk/',
            'source': source,
            'srv': parameter,
            'theme': 'multisite',
            'view': 0,
            'start': int(self.timerange[0].timestamp()),
            'end': int(self.timerange[1].timestamp()),
        }

        images = []

        for machine in self.machines:
            params['host'] = machine

            with warnings.catch_warnings():
                # Ignore warning about SSL certificates
                warnings.simplefilter('ignore')
                r = self.s.get(url, params=params)
            # b = io.BytesIO(r.content)
            # img = plt.imread(b)
            images.append(r.content)

        return images
