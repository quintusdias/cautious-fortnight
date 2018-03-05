# Standard library imports ...
import io
import pathlib

# Third party library imports ...
from lxml import etree

# 19115-2 XSD.  Use this for validation.
_SCHEMA_DOC = """
<!--
    Nov 20, 2009, AMilan
    changed targetNamespace to gmi and changed include to gmi.xsd

    Apr 29, 2016, JEvans
    Add install_root, to be interpolated to schema installation directory.
-->
<xs:schema
    targetNamespace="http://www.isotc211.org/2005/gmi"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:gmi="http://www.isotc211.org/2005/gmi"
    xmlns:srv="http://www.isotc211.org/2005/srv">

    <xs:include
        schemaLocation="{install_root}/schema/gmi/gmi.xsd"/>
    <xs:import
        namespace="http://www.isotc211.org/2005/gmd"
        schemaLocation="{install_root}/schema/gmd/gmd.xsd"/>
    <xs:import
        namespace="http://www.isotc211.org/2005/srv"
        schemaLocation="{install_root}/schema/srv/srv.xsd"/>

</xs:schema>
"""


class Validator(object):
    """
    Validates XML files according to 19115-2 schema.
    """
    def __init__(self):
        """
        Load the 19115-2 schema so that every XML file produced is validated.
        """
        # Interpolate the path to the XSD files.
        path = pathlib.Path(__file__).parent / 'data'
        schema_doc = _SCHEMA_DOC.format(install_root=path.as_uri())
        f = io.StringIO(schema_doc)
        self.schema = etree.XMLSchema(file=f)

    def validate(self, doc):
        """
        Validate the XML of the given schema.

        Parameters
        ----------
        doc : lxml.etree.ElementTree
           Document to be validated.
        """
        # Save a working copy so we can investigate after the fact.
        # filename = os.path.join(os.environ['HOME'], 'validate.xml')
        # with open(filename, 'wt') as fptr:
        #     fptr.write(etree.tostring(self.root).decode('utf-8'))
        self.schema.assertValid(doc)
