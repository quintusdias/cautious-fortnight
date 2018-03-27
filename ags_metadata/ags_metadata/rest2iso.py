# Standard library imports...
import copy
import datetime as dt
import itertools
import logging
import os
import pathlib
import re
import warnings

# Third-party library imports ...
from lxml import etree
import osr
import pkg_resources as pkg
import pyproj
import requests
import yaml

# Local imports...
from . import const
from .validator import Validator


# Use the EXSLT namespace for some more powerful XPATH paths.
_REGEX_NAMESPACE = {'re': "http://exslt.org/regular-expressions"}


def process_text_element(item):
    text = item.text

    for elt in item[:]:
        text += ' ' + elt.text.strip()
        text += elt.tail

    # Remove any leading or trailing newlines.
    text = text.strip()

    # Remove any interior newlines with a space.
    text = text.replace('\n', ' ')

    # Remove the trademark symbol.
    text = text.replace(chr(8482), '')

    return text


# Just map the method indicated by the user to a level in the logging package.
_logging_level = {
    'debug': logging.DEBUG,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}


class RestToIso(object):
    """
    Attributes
    ----------
    folder, service : str
        These uniquely identify the service.
    rest_html : str
        HTML from the service REST access point
    json : dict
        JSON from the service REST access point
    root : ElementTree
        Complete ISO 19115-2 document
    """

    def __init__(self, config_file, verbose='info', output_root=None):
        """
        Parameters
        ----------
        config_file : str or path
            path to YAML configuration file
        verbose : str
            corresponds to a logging package log level
        output_root : str
            Where to write the resulting records.
        """
        self.validate = True
        self.parser = etree.XMLParser(remove_blank_text=True)

        with open(config_file, 'rt') as f:
            self.config = yaml.load(f.read())

        self.session = requests.Session()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(_logging_level[verbose])

        # DEV and QA sites have trouble with SSL certs.  We allow for this
        # check to be suppressed.
        # self.session.verify = self.config['verify_ssl_cert']
        self.session.verify = False

        self.base_url = (f"https://{self.config['server']}"
                         f"/arcgis/rest/services/")

        if output_root is None:
            self.output_directory = pathlib.Path(self.config['server'])
        else:
            self.output_directory = pathlib.Path(output_root)

        self.validator = Validator()

    def load_service_metadata(self):
        """
        Retrieve the REST HTML, JSON for a single service.
        """
        url = (f"{self.base_url}"
               f"/{self.folder}/{self.service}/{self.service_type}")
        r = self.session.get(url)
        r.raise_for_status()
        self.rest_html = r.content.decode('utf-8')

        # Get the JSON.
        r = self.session.get(url, params={'f': 'pjson'})
        r.raise_for_status()
        self.json = r.json()

    def load_srs(self):
        """
        Get the spatial reference from the JSON for the current service.
        """
        # Special case where we cannot identify the SRID.
        if 'wkt' in self.json['spatialReference'].keys():
            if self.json['spatialReference']['wkt'] in const.UNKNOWN_WKTS:
                self.srs = None
                return

        if 'latestWkid' in self.json['spatialReference'].keys():
            # For nowCOAST, this is usually
            #
            # 'spatialReference': {
            #     "wkid": 102100,
            #     "latestWkid": 3857
            # }
            #
            self.srs = self.json['spatialReference']['latestWkid']
        elif 'wkid' in self.json['spatialReference'].keys():
            # wkid is the fall back if using IDs.
            self.srs = self.json['spatialReference']['wkid']
        else:
            # Ok, use well known text.
            s = osr.SpatialReference(wkt=self.json['spatialReference']['wkt'])
            if s.IsProjected():
                cs = s.GetAttrValue('projcs')
            else:
                cs = s.GetAttrValue('geogcs')
            self.srs = const.COORDINATE_SYSTEM_TO_EPSG[cs]

    def load_template(self):
        # Assumption is that a template file has been supplied.
        """
        The template is an XML file that has all the required elements present,
        but not filled out.
        """
        template = pkg.resource_filename(__name__, 'data/template.xml')
        self.tree = etree.parse(template, self.parser)
        self.root = self.tree.getroot()

    def retrieve_configuration_file_value(self, keyword_path):
        """
        Go through each configuration until we find the keyword.
        """
        # Drill down until we get the final value
        config = self.service_config
        for keyword in keyword_path:
            config = config[keyword]

        value = config

        if keyword_path[-1] in [
            'gmd:date__publication',
            'gmd:date__creation',
        ]:
            # It may be a datetime, it may be a string.  We want to
            # convert it to a string.
            try:
                value = value.strftime('%Y-%m-%d')
            except AttributeError:
                pass
        return value

    def get_element(self, path, root=None):
        """
        Parameters
        ----------
        path : str
            XPATH path of element in question.
        """
        if root is not None:
            elt = root.xpath(path, namespaces=self.root.nsmap)[0]
        else:
            elt = self.tree.xpath(path, namespaces=self.root.nsmap)[0]
        return elt

    def _append_reference(self, parent, url, name):

        elt_name = "{{{ns}}}onLine".format(ns=self.root.nsmap['gmd'])
        online = etree.Element(elt_name)

        elt_name = "{{{ns}}}CI_OnlineResource"
        elt_name = elt_name.format(ns=self.root.nsmap['gmd'])
        ci_onlineresource = etree.SubElement(online, elt_name)

        elt_name = "{{{ns}}}linkage".format(ns=self.root.nsmap['gmd'])
        linkage = etree.SubElement(ci_onlineresource, elt_name)

        elt_name = "{{{ns}}}URL".format(ns=self.root.nsmap['gmd'])
        url_elt = etree.SubElement(linkage, elt_name)
        url_elt.text = url

        elt_name = "{{{ns}}}name".format(ns=self.root.nsmap['gmd'])
        gmd_name = etree.SubElement(ci_onlineresource, elt_name)

        elt_name = "{{{ns}}}CharacterString".format(ns=self.root.nsmap['gco'])
        cs = etree.SubElement(gmd_name, elt_name)
        cs.text = name

        elt_name = "{{{ns}}}description".format(ns=self.root.nsmap['gmd'])
        description = etree.SubElement(ci_onlineresource, elt_name)

        elt_name = "{{{ns}}}CharacterString".format(ns=self.root.nsmap['gco'])
        cs = etree.SubElement(description, elt_name)
        cs.text = 'upstream reference'

        name = "{{{ns}}}function".format(ns=self.root.nsmap['gmd'])
        function = etree.SubElement(ci_onlineresource, name)

        name = "{{{ns}}}CI_OnLineFunctionCode"
        name = name.format(ns=self.root.nsmap['gmd'])
        functioncode = etree.SubElement(function, name)
        functioncode.attrib['codeList'] = (
            'http://www.ngdc.noaa.gov/metadata/published/xsd/schema'
            '/resources/Codelist/gmxCodelists.xml#CI_OnLineFunctionCode'
        )
        functioncode.attrib['codeListValue'] = 'information'
        functioncode.text = 'information'

        parent.append(online)

    def run(self):

        if not self.session.verify:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                self._run()
        else:
            self._run()

    def _run(self):

        # Get the REST directory forlder listing
        r = self.session.get(self.base_url, params={'f': 'pjson'})
        j = r.json()
        if 'error' in j.keys():
            msg = f'Could not get a REST directory listing from {r.url}.'
            raise IOError(msg)

        folders = j['folders']

        for folder in folders:
            self.process_folder(folder)

    def process_folder(self, folder):
        self.folder = folder

        self.logger.info(f'Processing {self.folder}...')

        # Are we configured for it?
        if self.folder not in [folder['name'] for folder in self.config['folder']]:
            # self.logger.warn(f'Skipping folder {self.folder}...')
            return

        config = [folder for folder in self.config['folder']
                  if folder['name'] == self.folder]
        config = config[0]

        url = f"{self.base_url}/{self.folder}"
        r = self.session.get(url, params={'f': 'pjson'})
        j = r.json()
        jservices = j['services']

        if len(jservices) == 0:
            # Empty?  nothing to do.
            return

        # Go ahead and create the output folder since we know we will
        # write to it.
        path = self.output_directory / self.folder
        path.mkdir(parents=True, exist_ok=True)

        for json_service_item in jservices:
            self.process_service(config, json_service_item)

    def process_service(self, folder_config, json_service_item):
        """
        Construct metadata for a service within a directory.
        """

        parts = json_service_item['name'].split('/')
        service = '/'.join(parts[1:])

        self.service = service
        self.service_type = json_service_item['type']

        if self.service_type not in ['MapServer', 'ImageServer']:
            msg = 'Skipping {} service types...'.format(self.service_type)
            self.logger.warn(msg)
            return

        # Are we configured for it?
        configs = [
            item for item in folder_config['service']
            if item['name'] in json_service_item['name']
        ]
        if len(configs) == 0:
            # No item in the configuration file for this service.
            # msg = 'Skipping {}/{}, not in config file...'
            # msg = msg.format(self.folder, self.service)
            # self.logger.warn(msg)
            return

        msg = f"Processing {self.folder}/{self.service}.{self.service_type}..."
        self.logger.info(msg)

        self.service_config = configs[0]

        self.load_template()

        self._update_service()

    def _update_service(self):

        self.load_service_metadata()
        self.load_srs()

        self.update_file_identifier()
        self.update_datestamp()
        self.update_reference_system_info()
        self.update_identification_info()
        self.update_distribution_info()
        self.update_data_quality_info()

        self.root.getroottree().write('to_validate.xml')

        if self.validate:
            self.validator.validate(self.root)

        # Write out to a file.
        path = self.output_directory / self.folder / (self.service + '.xml')
        self.root.getroottree().write(str(path), pretty_print=True)

    def is_time_enabled(self):
        """
        Right now, rely upon the user to know this.  If a file is time-enabled,
        there should be a "time_period" section in the confguration file.

        Returns
        -------
        bool
            True if the service is time-enabled, false otherwise.
        """
        try:
            self.retrieve_configuration_file_value('time_period')
            b = True
        except KeyError:
            b = False
        return b

    def update_abstract(self):
        """
        The service description can be extracted as HTML.  Join the <P> content
        together to make the abstract.
        """
        keyword_path = (
            'gmi:MI_Metadata',
            'gmd:identificationInfo',
            'srv:SV_ServiceIdentification',
            'gmd:abstract'
        )

        try:
            text = self.retrieve_configuration_file_value(keyword_path)
        except KeyError:
            try:
                doc = etree.HTML(self.json['serviceDescription'])
                text = '\n\n'.join(text for text in doc.xpath('body/p/text()'))
            except etree.XMLSyntaxError:
                text = self.json['description']

        elt = self.get_element(const.ABSTRACT)
        elt.text = text

    def update_aggregation_info(self):
        """
        Just remove it for the moment.
        """
        elt = self.get_element(const.AGGREGATION_INFO)
        elt.getparent().remove(elt)

    def update_browse_graphic(self):
        """
        Fill in the gmd:MD_BrowseGraphic element.

        Just use the export image for this.
        """
        doc = etree.HTML(self.rest_html)
        try:
            elt = doc.xpath('//a[text()="Export Map"]')[0]
        except IndexError:
            elt = doc.xpath('//a[text()="Export Image"]')[0]
        finally:
            # The HREF attribute has the path.
            path = elt.attrib['href']

        # Append the response format, because otherwise an HTML page is
        # returned.
        elt = self.root.xpath(const.BROWSE_GRAPHIC_FILENAME,
                              namespaces=self.root.nsmap)[0]
        elt.text = f"https://{self.config['server']}{path}&f=image"


        elt = self.root.xpath(const.BROWSE_GRAPHIC_FILETYPE,
                              namespaces=self.root.nsmap)[0]
        elt.text = 'PNG'

    def update_contains_operations_wms_get_capabilities(self):
        """
        Fills in the srv:connectPoint element - WMS operations performed by
        the service.
        """

        # WMS Connect point URL
        elt = self.get_element(const.WMS_CONNECT_POINT_URL)
        url = (
            f"http://{self.config['server']}"
            f"/arcgis/services/{self.folder}/{self.service}/MapServer"
            f"/WMSServer?request=GetCapabilities%26service=WMS"
        )
        elt.text = url

        # WMS Connect point name
        elt = self.get_element(const.WMS_CONNECT_POINT_NAME)
        elt.text = f"{self.config['project']} {self.service} Connect Point"

        # WMS Connect point description
        elt = self.get_element(const.WMS_CONNECT_POINT_DESCRIPTION)
        elt.text = (
            f"WMS GetCapabilities endpoint for "
            f"{self.config['project']} {self.service} OGC Web Map Service"
        )

    def update_citation(self):
        self.update_title()
        self.update_creation_date()
        self.update_publication_date()
        self.update_revision_date()

    def update_creation_date(self):
        """
        Date identifies when the resource was brought into existance.

        It should be specified by the user via the configuration file.  For
        most nowCOAST services, it is when nowCOAST went live, Sept 21, 2015.
        The inundation service came later, though.
        """
        elt = self.get_element(const.CREATION_DATE)

        keyword_path = (
            'gmi:MI_Metadata',
            'gmd:identificationInfo',
            'srv:SV_ServiceIdentification',
            'gmd:citation',
            'gmd:CI_Citation',
            'gmd:date__creation'
        )

        date = self.retrieve_configuration_file_value(keyword_path)
        elt.text = date

    def update_data_quality_info(self):
        self.update_lineage()

    def update_identification_info(self):
        self.update_citation()
        self.update_abstract()
        self.update_browse_graphic()
        self.update_descriptive_keywords()
        self.update_descriptive_keywords__gcmd_place()
        # self.update_descriptive_keywords__iso_temporal()
        self.update_descriptive_keywords__wmo_theme()
        # self.update_aggregation_info()
        self.update_service_type_and_service_version()
        self.update_geographic_bounding_box()
        self.update_temporal_extents()
        self.update_contains_operations_rest_endpoint()
        self.update_contains_operations_wms_get_capabilities()

    def update_service_type_and_service_version(self):
        """
        Identify the ArcGIS server version.
        """
        path = '/'.join([
            'gmd:identificationInfo',
            'srv:SV_ServiceIdentification',
            'srv:serviceTypeVersion',
            'gco:CharacterString'
        ])
        elt = self.get_element(path)
        elt.text = str(self.json['currentVersion'])

    def update_contains_operations_rest_endpoint(self):
        """
        Fills in the srv:connectPoint element - REST operations performed by
        the service.
        """
        # Connect point URL
        elt = self.get_element(const.REST_ENDPOINT_CONNECT_POINT_URL)
        url = (f"https://{self.config['server']}"
               f"/arcgis/rest/services/{self.folder}/{self.service}/MapServer")
        elt.text = url

        # Connect point name
        elt = self.get_element(const.REST_ENDPOINT_CONNECT_POINT_NAME)
        elt.text = f"{self.service} Map Service Connect Point"

        # Connect point description
        elt = self.get_element(const.REST_ENDPOINT_CONNECT_POINT_DESCRIPTION)
        elt.text = (
            f"REST endpoint for {self.config['project']} {self.service} "
            f"Map Service"
        )

    def update_datestamp(self):
        """
        Metadata creation date or last update.  We're going with last update,
        which is right now.
        """
        elt = self.get_element(const.DATE_STAMP)
        elt.text = dt.datetime.now().strftime('%Y-%m-%d')

    def _update_desc_keywords(self, path, cfg_keyword):
        """
        Parameters
        ----------
        path : str
            Path to descriptiveKeywords XML element.
        cfg_keyword : str
            Keyword name to be looked up in the YAML configuration file.
        """
        root = self.get_element(path)

        keyword_path = (
            'gmi:MI_Metadata',
            'gmd:identificationInfo',
            cfg_keyword
        )

        try:
            keywords = self.retrieve_configuration_file_value(keyword_path)
        except KeyError:
            # The user didn't specify this, so delete it.
            root.getparent().remove(root)
            return

        # Ok now locate the MD_Keywords element and insert keywords into it.
        md_keywords_elt = self.get_element('gmd:MD_Keywords', root=root)
        for idx, keyword in enumerate(keywords):

            eltname = "{{{ns}}}keyword".format(ns=self.root.nsmap['gmd'])
            keyword_elt = etree.SubElement(md_keywords_elt, eltname)

            eltname = "{{{ns}}}CharacterString"
            eltname = eltname.format(ns=self.root.nsmap['gco'])
            char_elt = etree.SubElement(keyword_elt, eltname)
            char_elt.text = keyword

            # Insert them into position, all must come before gmd:type
            md_keywords_elt.insert(idx, keyword_elt)

    def update_descriptive_keywords(self):
        """
        Process keywords for the gmd:descriptiveKeywords.

        This variant does not have the xlink:title attribute.  The keywords
        come straight from the REST metadata.
        """
        root = self.get_element(const.DESCRIPTIVE_KEYWORDS)
        md_keywords = root.getchildren()[0]

        try:
            keywords = self.json['documentInfo']['Keywords'].split(',')
        except KeyError:
            # Ok, there are no keywords.  Remove the descriptive keywords
            # element.
            root.getparent().remove(root)
            return

        # Now go through each service level keyword, add it to the list.
        for keyword in reversed(keywords):

            name = "{{{ns}}}keyword".format(ns=self.root.nsmap['gmd'])
            keyword_elt = etree.Element(name)

            name = "{{{ns}}}CharacterString".format(ns=self.root.nsmap['gco'])
            char_elt = etree.SubElement(keyword_elt, name)
            char_elt.text = keyword

            # The keywords must preceed the gmd:type element.
            md_keywords.insert(0, keyword_elt)

    def update_descriptive_keywords__gcmd_place(self):
        """
        Process keywords for the gmd:descriptiveKeywords, GCMD Place.
        """
        self._update_desc_keywords(const.DESCRIPTIVE_KEYWORDS__GCMD_PLACE,
                                   'gmd:descriptiveKeywords__gcmd_place')

    def update_descriptive_keywords__iso_temporal(self):
        self._update_desc_keywords(const.DESCRIPTIVE_KEYWORDS__ISO_TEMPORAL,
                                   'gmd:descriptive_keywords__iso_temporal')

    def update_descriptive_keywords__wmo_theme(self):
        """
        WMO keywords must be supplied via config file if at all.
        """
        try:
            self._update_desc_keywords(const.DESCRIPTIVE_KEYWORDS__WMO_THEME,
                                       'gmd:descriptiveKeywords__wmo_theme')
        except IndexError:
            pass

    def update_distribution_info(self):
        self.update_transfer_options()

    def update_file_identifier(self):
        """
        A unique phrase or string which uniquely identifies the metadata file.

        The assumption is that the string "folder.service" being added to
        what's specified in the configuration file.  That makes this item
        different from other elements; the other elements usually have there
        content completely replaced.  Here we are just adding to it.
        """
        elt = self.get_element(const.FILE_IDENTIFIER)

        keyword_path = (
            "gmi:MI_Metadata",
            "gmd:fileIdentifier",
        )

        prefix = self.retrieve_configuration_file_value(keyword_path)
        elt.text = f"{prefix}:{self.folder}.{self.service}"

    def _get_extent(self):
        return self.json['fullExtent']

    def update_geographic_bounding_box(self):
        """
        geographic position of the dataset
        """
        # data is included in the bounding box, so the value is 1
        elt = self.get_element(const.EXTENT_TYPE_CODE)
        elt.text = '1'

        extent = self._get_extent()

        if self.srs is None:
            # We already have the extents, as it is geographic.
            lonmin = extent['xmin']
            lonmax = extent['xmax']
            latmin = extent['ymin']
            latmax = extent['ymax']

        else:

            if self.srs in [54012, 102003]:
                # No EPSG code for this one, must use proj4 text.
                in_proj = pyproj.Proj(const.PROJ4TEXT[self.srs])
            else:
                in_proj = pyproj.Proj(init=f'epsg:{self.srs}')
            out_proj = pyproj.Proj(init='epsg:4326')

            xmin = extent['xmin']
            ymin = extent['ymin']
            lonmin, latmin = pyproj.transform(in_proj, out_proj, xmin, ymin)

            xmax = extent['xmax']
            ymax = extent['ymax']
            lonmax, latmax = pyproj.transform(in_proj, out_proj, xmax, ymax)

        if (((self.srs == 3857) and
             (lonmin < 0) and ((180 - abs(lonmin)) < 1e-2) and
             (lonmax < 0) and ((xmax - xmin) > 4e7) and
             ((180 - abs(lonmax)) < 1e-2))):
            # Special situation where the coverage is global.  If the x
            # extents are used as-is, the xmax value (which project to
            # ~180) rolls over into negative longitude territory.
            lonmin = -180
            lonmax = 180

        elt = self.get_element(const.WEST_BOUNDING_LONGITUDE)
        elt.text = str(lonmin)

        elt = self.get_element(const.SOUTH_BOUNDING_LATITUDE)
        elt.text = str(latmin)

        elt = self.get_element(const.EAST_BOUNDING_LONGITUDE)
        elt.text = str(lonmax)

        elt = self.get_element(const.NORTH_BOUNDING_LATITUDE)
        elt.text = str(latmax)

    def update_lineage(self):
        self.update_lineage_source()
        self.update_lineage_statement()
        self.update_process_steps()

    def _retrieve_rest_references(self):
        """
        No IDP-GIS support yet.
        """
        return []

    def update_additional_references(self):
        """
        Must be loaded from configuration file.
        """
        root = self.get_element(const.TRANSFER_OPTIONS)

        try:
            ymlpath = (
                'gmi:MI_Metadata',
                'gmd:distributionInfo',
                'gmd:MD_Distribution',
                'gmd:transferOptions',
                'gmd:MD_DigitalTransferOptions',
                'gmd:onLine__references',
            )
            references = self.retrieve_configuration_file_value(ymlpath)
        except KeyError:
            # Don't delete the root, just do nothing.  It's quite possible
            # there aren't any specified.
            return

        for idx, reference in enumerate(references):
            self._append_reference(root,
                                   reference['gmd:URL'],
                                   reference['gmd:name'])

    def update_wms_get_capabilities_for_download(self):
        """
        Fill in the WMS portion of the MD_DigitalTransferOptions.
        """
        root = self.get_element(const.TRANSFER_OPTIONS)
        path = '/'.join([
            'gmd:onLine',
            'gmd:CI_OnlineResource[@id="WMS"]',
            'gmd:linkage',
            'gmd:URL'
        ])
        elt = self.get_element(path, root=root)

        url = (
            f"https://{self.config['server']}"
            f"/arcgis/services/{self.folder}/{self.service}"
            "/MapServer/WMSServer"
            "?"
            "request=GetCapabilities%26service=WMS"
        )

        elt.text = url

        # Update the description
        path = '/'.join([
            'gmd:onLine',
            'gmd:CI_OnlineResource[@id="WMS"]',
            'gmd:description',
            'gco:CharacterString',
        ])
        elt = self.get_element(path, root=root)

        text = 'Capabilities file for the {project} {service} WMS server'
        elt.text = text.format(project=self.config['project'],
                               service=self.service)

    def update_publication_date(self):
        """
        Date identifies when the resource was issued.
        """
        elt = self.get_element(const.PUBLICATION_DATE)

        keyword_path = (
            'gmi:MI_Metadata',
            'gmd:identificationInfo',
            'srv:SV_ServiceIdentification',
            'gmd:citation',
            'gmd:CI_Citation',
            'gmd:date__publication'
        )

        date = self.retrieve_configuration_file_value(keyword_path)
        elt.text = date

    def update_revision_date(self):
        """
        Date identifies when the resource was examined or re-examined and
        improved or amended.  This would be right now!
        """
        elt = self.get_element(const.REVISION_DATE)
        elt.text = dt.datetime.now().strftime('%Y-%m-%d')

    def update_reference_system_info(self):
        """
        Only 3857, 4269, and 4326 SRIDs are recognized at this time.
        """
        elt = self.get_element(const.REFERENCE_SYSTEM_INFO)

        if self.srs is None:
            # We don't know, so delete the element.  It's optional.
            elt.getparent().remove(elt)

        elif self.srs in [3857, 4269, 4326]:
            # Link in to Docucomp because it has already defined the spatial
            # reference.
            title = const.DOCUCOMP[self.srs]['title']
            attrib_key = "{{{ns}}}title".format(ns=self.root.nsmap['xlink'])
            elt.attrib[attrib_key] = title

            url = const.DOCUCOMP[self.srs]['url']
            attrib_key = "{{{ns}}}href".format(ns=self.root.nsmap['xlink'])
            elt.attrib[attrib_key] = url

        elif self.srs in [2253, 4152, 4759, 26918, 54012, 102003]:
            # Use our own definitions.
            relname = os.path.join('snippets', str(self.srs) + '.xml')
            filename = pkg.resource_filename(__name__, relname)
            tree = etree.parse(filename, self.parser)
            root = tree.getroot()
            elt.append(root)

            attrib_key = "{{{ns}}}title".format(ns=self.root.nsmap['xlink'])
            elt.attrib[attrib_key] = 'North American Datum 1983'

        else:
            fmt = 'This spatial reference ({code}) is not supported'
            msg = fmt.format(code=self.srs)
            raise RuntimeError(msg)

    def update_process_steps(self):
        """
        There could be a series of process steps.
        """
        try:
            path = (
                'gmi:MI_Metadata',
                'gmd:dataQualityInfo',
                'gmd:DQ_DataQuality',
                'gmd:lineage',
                'gmd:LI_Lineage',
                'gmd:processStep'
            )
            steps = self.retrieve_configuration_file_value(path)
        except KeyError:
            # Just ignore this element if we don't find it in the configuration
            # file.
            return

        # Isolate the parent element.
        parent = self.root.xpath(const.LI_LINEAGE,
                                 namespaces=self.root.nsmap)[0]

        # Should be the single child element.
        item = parent.xpath('gmd:processStep', namespaces=self.root.nsmap)[0]

        # Duplicate for each additional process step.
        for idx in range(1, len(steps)):
            parent.insert(idx, copy.deepcopy(item))

        # Isolate the element in question.
        path = '/'.join([
            'gmd:processStep',
            'gmd:LI_ProcessStep',
            'gmd:description',
            'gco:CharacterString',
        ])
        elts = parent.xpath(path, namespaces=self.root.nsmap)

        # Insert the text for the processing step.
        for elt, process_step in zip(elts, steps):
            elt.text = process_step

    def update_lineage_source(self):
        try:
            elt = self.get_element(const.LINEAGE_SOURCE)
        except IndexError:
            return
        try:
            path = (
                'gmi:MI_Metadata',
                'gmd:dataQualityInfo',
                'gmd:DQ_DataQuality',
                'gmd:lineage',
                'gmd:LI_Lineage',
                'gmd:source'
            )
            source = self.retrieve_configuration_file_value(path)
        except KeyError:
            # The user didn't specify this, so delete it.
            elt.getparent().remove(elt)
            return
        elt.text = source

    def update_lineage_statement(self):
        elt = self.get_element(const.LINEAGE_STATEMENT)
        try:
            path = (
                'gmi:MI_Metadata',
                'gmd:dataQualityInfo',
                'gmd:DQ_DataQuality',
                'gmd:lineage',
                'gmd:LI_Lineage',
                'gmd:statement'
            )
            txt = self.retrieve_configuration_file_value(path)
        except KeyError:
            # keep the default
            return
        elt.text = txt

    def update_temporal_extents(self):
        """
        Just remove for now, it's not mandatory.
        """
        root = self.get_element(const.TIME_PERIOD)
        root.getparent().remove(root)

    def update_title(self):
        """
        Name by which the cited resource is known.
        """
        try:
            text = self.retrieve_configuration_file_value('title')
        except KeyError:
            try:
                text = self.json['mapName']
                fmt = 'Map Service for "{map_name}"'
            except KeyError:
                text = self.json['name']
                fmt = 'Image Service for "{map_name}"'
            text = fmt.format(map_name=text)

        elt = self.get_element(const.TITLE)
        elt.text = text

    def update_transfer_options(self):
        self.update_wms_get_capabilities_for_download()
        self.update_additional_references()

    def __str__(self):
        msg = etree.tostring(self.tree, encoding='utf-8', pretty_print=True)
        msg = msg.decode('utf-8')
        return msg


class NowCoastRestToIso(RestToIso):

    def __init__(self, config_file, verbose='info'):
        super().__init__(config_file, verbose=verbose)

    def _retrieve_rest_references(self):
        """
        Retrieve list of references defined in REST.  Each item returned is
        a dictionary of the descriptive text and the URL for the reference.

        We are looking for elements that look like

        <h4>References</h4>
        <ul>
          <li> stuff1 (Available at <a href="URL">someplace1</a> </li>
          <li> stuff2 (Available at <a href="URL">someplacer2</a> </li>
          .
          .
          .
        </ul>
        """

        references = []

        # Try to get it from the REST data
        doc = etree.HTML(self.json['serviceDescription'])

        # Find <LI> elements whose <UL> parent directly follow an
        # <h4> or <h5> element whose text is "References".  Yeah!
        path = (
            ".//*[re:test(local-name(), 'h[45]')][text()='References']"
            "/following-sibling::ul"
            "/li"
        )
        elts = doc.xpath(path, namespaces=_REGEX_NAMESPACE)

        for li in elts:

            # Is there a URL?
            urls = li.xpath('a')
            if len(urls) == 0:
                # No URL present.  That's required, so just go on to the
                # next.
                continue

            url = urls[0].attrib['href']

            # Get rid of the leading text inside the parenthesis.  The
            # URL follows this, which makes the <LI> mixed content.
            text = re.sub('\s*\(Available\s+(at|from)\s+', '', li.text)

            # Get rid of any tabs, newlines, or multiple spaces.
            text = text.replace('\n', '')
            text = text.replace('\t', '')
            text = text.replace('\s{2,}', ' ')
            self.logger.debug(text)

            references.append({'URL': url, 'name': text})

        return references

    def _extract_nowcoast_abstract(self):
        """
        Extract the abstract from the service description HTML.
        """

        doc = etree.HTML(self.json['serviceDescription'])
        div = doc.xpath('body/div')[0]

        # lxml is easier than XPATH here.
        def predicate(x):
            x.text != "Time Information"

        lst = []

        # Find all the elements whose text is NOT "Time Information".
        for elt in itertools.takewhile(predicate, div.iterchildren()):

            # Change something like
            #
            # <h4>Stuff</h4>
            #    <p>stuff explanation</p>
            #
            # into
            #
            # Stuff: stuff explanation
            if elt.tag in ['h4', 'h5']:
                text = elt.text + ':'
            elif elt.tag == 'p':
                text = process_text_element(elt)
            else:
                continue

            lst.append(text)

        text = '\n\n'.join(lst)

        # Get rid of any sequences of two or more spaces.  Don't do that for
        # any whitespace, because two newlines in a row might be ok.
        text = re.sub('  {2,}', ' ', text)

        return text

    def update_abstract(self):
        """
        Try to get the configuration file setting first.  If it's not there,
        try to get it from the service metadata
        """

        keyword_path = (
            'gmi:MI_Metadata',
            'gmd:identificationInfo',
            'srv:SV_ServiceIdentification',
            'gmd:abstract'
        )

        try:
            text = self.retrieve_configuration_file_value(keyword_path)
        except KeyError:
            try:
                text = self._extract_nowcoast_abstract()
            except IndexError as e:
                # We have no information available.  Just use the service name.
                msg = (
                    f"Unable to extract nowcoast abstract.  Using the service "
                    f"name instead.\n\n"
                    f"{repr(e)}"
                )
                self.logger.critical(msg)
                text = f"{self.folder}/{self.service}.{self.service_type}"
            except etree.XMLSyntaxError:
                # We have no information available.  Just use the service name.
                text = f"{self.folder}/{self.service}.{self.service_type}"
            except AttributeError as e:
                # We have no information available.  Just use the service name.
                self.logger.critical(repr(e))
                text = f"{self.folder}/{self.service}.{self.service_type}"

        # Include the time information if appropriate.
        if self.is_time_enabled():
            text += '\n\n' + const.TIME_INFORMATION

        elt = self.get_element(const.ABSTRACT)
        elt.text = text

    def update_temporal_extents(self):
        """
        Update the time period element.  The goal is to produce something like
        this.

        <gml:TimePeriod gml:id="timePeriod1">
            <gml:beginPosition indeterminatePosition="now"/>
            <gml:endPosition indeterminatePosition="after"/>
            <gml:timeInterval unit="minute">1</gml:timeInterval>
        </gml:TimePeriod>

        """
        try:
            root = self.get_element(const.TIME_PERIOD)
        except IndexError:
            return

        keyword_path = (
            'gmi:MI_Metadata',
            'gmd:identificationInfo',
            'srv:SV_ServiceIdentification',
            'srv:extent',
            'gmd:EX_Extent',
            'gmd:temporalElement',
            'gmd:EX_TemporalExtent',
            'gmd:extent',
            'gml:TimePeriod'
        )

        try:
            time_period = self.retrieve_configuration_file_value(keyword_path)
        except KeyError:
            # The user didn't specify this, so delete it.
            root.getparent().remove(root)
            return

        # Set the ID
        key = f"{{{self.root.nsmap['gml']}}}id"
        root.attrib[key] = 'timePeriod1'

        # No description at the moment.  It is optional anyway.

        elt = root.xpath('gml:beginPosition', namespaces=self.root.nsmap)[0]
        elt.attrib['indeterminatePosition'] = time_period['gml:beginPosition']

        elt = root.xpath('gml:endPosition', namespaces=self.root.nsmap)[0]
        elt.attrib['indeterminatePosition'] = time_period['gml:endPosition']

        elt = root.xpath('gml:timeInterval', namespaces=self.root.nsmap)[0]
        if 'time_interval' in time_period.keys():
            elt.text = str(time_period['gml:timeInterval'])
        else:
            # delete it.
            elt.getparent().remove(elt)

    def update_title(self):
        """
        Name by which the cited resource is known.  The goal is for this to
        look something like

        <gmd:title>
            <gco:CharacterString>
                nowCOAST's Map Service for NOAA NWS Weather Warnings for
                Short-Duration Hazards in Inland, Coastal,
                and Maritime Areas (Time Enabled)
            </gco:CharacterString>
        </gmd:title>
        """
        text = (f"{self.config['project']} Map Service "
                f"for {self.json['mapName']}")

        elt = self.get_element(const.TITLE)
        elt.text = text
