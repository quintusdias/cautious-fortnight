# Standard library imports
from copy import deepcopy
import pathlib

# 3rd party library imports
from lxml import etree
import pkg_resources as pkg
import yaml


class GenerateTestPlans(object):
    """
    Create set of jmeter test plans from a configuration file.
    """

    def __init__(self, configfile):

        # Load our specific configuration file.
        with open(configfile, mode='rt') as f:
            self.config = yaml.load(f)

        # Load the test plan template.
        path = pkg.resource_filename(__name__, 'etc/plan.jmx')
        self.template = etree.parse(path)

        # Remove two important elements from the template.  We will use these
        # sub templates over and over again.
        path = 'hashTree/hashTree/ThreadGroup'
        source = self.template.xpath(path)[0]
        self.threadgroup_template = deepcopy(source)
        source.getparent().remove(source)

        path = 'hashTree/hashTree/hashTree'
        source = self.template.xpath(path)[1]
        self.wms_hashtree_template = deepcopy(source)
        source.getparent().remove(source)

    def run(self):
        for idx, _ in enumerate(self.config['intervals']):
            self.doc = deepcopy(self.template)
            self.transform(idx)

    def setup_simple_data_writer(self, ht, testunit, idx):
        """
        Setup the Simple Data Writer output element.
        """

        path = f"output/{idx:02d}/{testunit['service']}.csv"
        dest = pathlib.Path(path)
        dest.parents[0].mkdir(parents=True, exist_ok=True)

        expath = ('ResultCollector[@testname="Simple Data Writer"]'
                  '/stringProp[@name="filename"]')
        elts = ht.xpath(expath)
        elts[0].text = str(dest)

    def setup_http_request_defaults_web_server(self, ht, testunit):
        """
        Setup the "Web Server" panel in the HTTP Request Defaults element.

        Set the target server and port.  The port should not be used unless we
        are load testing just a single VM.
        """
        path = 'ConfigTestElement/stringProp[@name="HTTPSampler.domain"]'
        elts = ht.xpath(path)
        elts[0].text = self.config['server']

        path = 'ConfigTestElement/stringProp[@name="HTTPSampler.port"]'
        elts = ht.xpath(path)
        elts[0].text = str(self.config['port'])

    def setup_name(self, tg, testunit):
        """
        Setup the name element for this test unit.  This is what shows up in
        the left pane of the JMeter GUI.
        """
        tg.attrib['testname'] = f"{testunit['service']}"

    def setup_http_request_defaults_path(self, ht, testunit):
        """
        Setup the Path box in the HTTP Request panel in the HTTP Request
        Defaults element.
        """
        path = 'ConfigTestElement/stringProp[@name="HTTPSampler.path"]'
        elt = ht.xpath(path)[0]

        if testunit['service'].endswith('ImageServer'):
            export_type = 'exportImage'
        else:
            export_type = 'export'
        path = f"/arcgis/rest/services/{testunit['service']}/{export_type}"
        elt.text = path

    def setup_csv_input(self, ht, testunit):
        """
        Setup the CSV Data Set Config element.
        """

        # Set the path to the CSV Filename.
        expath = 'CSVDataSet/stringProp[@name="filename"]'
        elt = ht.xpath(expath)[0]

        folder, service, _ = testunit['service'].split('/')
        basename = testunit['service'] + '.csv'
        filepath = pathlib.Path(self.config['input_root']) / basename
        elt.text = str(filepath)

        # Set the variable names.
        elt = ht.xpath('CSVDataSet/stringProp[@name="variableNames"]')[0]
        elt.text = 'width,height,bboxsr,bbox,layers'

    def setup_http_request_name(self, ht, name):
        """
        """
        elt = ht.xpath('HTTPSamplerProxy')[0]
        elt.attrib['testname'] = name

    def setup_http_request(self, ht, testunit):
        """
        Setup the HTTP Request element.
        """
        self.setup_http_request_name(ht, 'REST Parameters')
        self.setup_http_request_parameters(ht)

    def setup_http_request_defaults(self, ht, testunit):
        """
        Setup the HTTP Requests Default element.
        """
        self.setup_http_request_defaults_web_server(ht, testunit)
        self.setup_http_request_defaults_path(ht, testunit)
        self.setup_rest_http_request_defaults_parameters(ht, testunit)

    def setup_http_request_parameters(self, ht):
        """
        Setup the Parameters tab in the HTTP Request panel in the
        HTTP Request element.  These are NOT default elements.
        """
        path = 'HTTPSamplerProxy/elementProp/collectionProp'
        elt = ht.xpath(path)[0]

        self.set_parameter(elt, 'width', '${width}')
        self.set_parameter(elt, 'height', '${height}')
        self.set_parameter(elt, 'bboxsr', '${bboxsr}')
        self.set_parameter(elt, 'bbox', '${bbox}')
        self.set_parameter(elt, 'layers', '${layers}')

    def setup_rest_http_request_defaults_parameters(self, ht, testunit):
        """
        Setup the Parameters tab in the HTTP Request panel in the
        HTTP Request Defaults element, REST endpoints only.
        """
        elt = ht.xpath('ConfigTestElement/elementProp/collectionProp')[0]

        # Looks like we only need to present f=image for all requests?
        self.set_parameter(elt, 'f', 'image')

    def setup_http_request_defaults_parameters_wms(self, ht, testunit):
        """
        Setup the Parameters tab in the HTTP Request panel in the
        HTTP Request Defaults element, WMS endpoints only.
        """
        elt = ht.xpath('ConfigTestElement/elementProp/collectionProp')[0]

        self.set_parameter(elt, 'service', 'WMS')
        self.set_parameter(elt, 'version', '1.3.0')
        self.set_parameter(elt, 'request', 'GetMap')

        parts = testunit['service'].split('/')
        self.set_parameter(elt, 'layers', f"{parts[0]}:{parts[1]}")

        self.set_parameter(elt, 'styles')
        self.set_parameter(elt, 'srs', 'EPSG:4326')
        self.set_parameter(elt, 'format', 'image/png')

    def set_parameter(self, parent, param_name, param_value=None):
        """
        For example, the style parameter would look like
            <elementProp name="format" elementType="HTTPArgument">
                <boolProp name="HTTPArgument.always_encode">false</boolProp>
                <stringProp name="Argument.value"/>
                <stringProp name="Argument.metadata">=</stringProp>
                <boolProp name="HTTPArgument.use_equals">true</boolProp>
                <stringProp name="Argument.name">styles</stringProp>
            </elementProp>

        """
        eProp = etree.SubElement(parent, 'elementProp')
        eProp.attrib['name'] = 'format'
        eProp.attrib['elementType'] = 'HTTPArgument'

        stringProp = etree.SubElement(eProp, 'stringProp')
        stringProp.attrib['name'] = 'Argument.name'
        stringProp.text = param_name

        stringProp = etree.SubElement(eProp, 'stringProp')
        stringProp.attrib['name'] = 'Argument.value'
        if param_value is not None:
            stringProp.text = param_value

        boolProp = etree.SubElement(eProp, 'boolProp')
        boolProp.attrib['name'] = 'HTTPArgument.always_encode'
        boolProp.text = 'false'

        stringProp = etree.SubElement(eProp, 'stringProp')
        stringProp.attrib['name'] = 'Argument.metadata'
        stringProp.text = '='

        boolProp = etree.SubElement(eProp, 'boolProp')
        boolProp.attrib['name'] = 'HTTPArgument.use_equals'
        boolProp.text = 'true'

    def setup_thread_properties(self, thread_group, testunit, idx):
        """
        Setup the Thread Properties panel in the Thread Group element.

        For now, just set the number of threads (concurrent users) for this
        level and the ramp up period.
        """
        path = 'stringProp[@name="ThreadGroup.num_threads"]'
        elt = thread_group.xpath(path)[0]
        elt.text = str(testunit['num_threads'][idx])

        path = 'stringProp[@name="ThreadGroup.ramp_time"]'
        elt = thread_group.xpath(path)[0]
        elt.text = str(self.config['ramp_time'])

    def setup_threadgroup(self, testunit, idx):
        thread_group = deepcopy(self.threadgroup_template)
        self.setup_name(thread_group, testunit)
        self.setup_thread_properties(thread_group, testunit, idx)
        return thread_group

    def transform(self, idx):

        e = self.doc.xpath('hashTree/hashTree')[0]

        for testunit in self.config['testunits']:

            tg = self.setup_threadgroup(testunit, idx)
            e.insert(2, tg)

            ht = deepcopy(self.wms_hashtree_template)
            self.setup_http_request_defaults(ht, testunit)
            self.setup_csv_input(ht, testunit)
            self.setup_http_request(ht, testunit)
            self.setup_simple_data_writer(ht, testunit, idx)

            e.insert(3, ht)

        # Create the test plan for this level.
        path = f"{self.config['output_root']}/plan_{idx:02d}.jmx"
        path = f"plan_{idx:02d}.jmx"
        self.plan = path
        with open(self.plan, 'wb') as f:
            self.doc.write(f, encoding='utf-8', xml_declaration=True,
                           pretty_print=True)
