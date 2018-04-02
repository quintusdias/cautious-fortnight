# Standard library imports
import itertools
import re

# 3rd party library imports
from lxml import etree

# Local imports
from . import const
from .rest2iso import RestToIso, process_text_element

_REGEX_NAMESPACE = {'re': "http://exslt.org/regular-expressions"}


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

    def update_contact(self):
        """
        This is an external record on docucomp.

        We need to override the inherited method because it exists for nowCOAST
        but not for other projects such as IDPGIS.
        """
        pass

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
        root = self.get_element(const.TIME_PERIOD)

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
