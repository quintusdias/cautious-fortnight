"""
Transform directory of ISO 19115-2 XML files to HTML.
"""

# Standard library imports
import pathlib

# Third party library imports
from lxml import etree
import pkg_resources as pkg


class ISO191152_to_HTML(object):
    """
    Attributes
    ----------
    input_path, output_path : str
        Paths to input XML files and output HTML directory.
    transform :
        Facilitates XSL transform from ISO 19115-2 XML to HTML.
    """
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path

        # Load the XSL
        relpath = pathlib.Path('data') / 'xsl' / 'xml-to-html-ISO.xsl'
        filename = pkg.resource_filename(__name__, str(relpath))
        doc = etree.parse(filename)
        self.transform = etree.XSLT(doc)

    def run(self):
        """
        Go over each XML file, transform to HTML.  Keep the directory structure
        the same.
        """
        input_root = pathlib.Path(self.input_path)
        output_root = pathlib.Path(self.output_path)

        for xml_path in input_root.rglob('*.xml'):

            # We want a relative path tacked onto the output root (which could
            # be relative... or possibly not.
            if xml_path.is_absolute:
                p = input_root.parts[-1] / xml_path.relative_to(input_root)
                output_path = (output_root / p).with_suffix('.html')
            else:
                output_path = (output_root / xml_path).with_suffix('.html')

            print(str(output_path))

            input_doc = etree.parse(str(xml_path))
            output_doc = self.transform(input_doc)

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_doc.getroot().getroottree().write(str(output_path))
