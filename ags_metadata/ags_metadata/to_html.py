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
    def __init__(self, input_path, outdir, logger=None):
        self.input_path = input_path

        self.setup_logger(logger)

        if isinstance(outdir, str):
            outdir = pathlib.Path(outdir)

        self.output_path = outdir
        if not self.output_path.exists():
            self.output_path.mkdir(parents=True, exist_ok=True)

        # Load the XSL
        relpath = pathlib.Path('data') / 'xsl' / 'xml-to-html-ISO.xsl'
        filename = pkg.resource_filename(__name__, str(relpath))
        doc = etree.parse(filename)
        self.transform = etree.XSLT(doc)

    def setup_logger(self, logger):
        if logger is None:
            self.logger = logging.getLogger('iso2html')
            self.logger.setLevel(logging.INFO)

            format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            formatter = logging.Formatter(format)
            self.logger.setFormatter(formatter)

        else:
            self.logger = logger

    def run(self):
        """
        Go over each XML file, transform to HTML.  Keep the directory structure
        the same.
        """
        self.logger.info('Now creating HTML from the ISO metadata.')

        input_root = pathlib.Path(self.input_path)
        output_root = pathlib.Path(self.output_path)

        for xml_path in input_root.rglob('*.xml'):

            output_path = xml_path.relative_to(input_root).with_suffix('.html')
            output_path = output_root / output_path

            msg = f'Writing HTML to {output_path}.'
            self.logger.info(msg)

            input_doc = etree.parse(str(xml_path))
            output_doc = self.transform(input_doc)

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_doc.getroot().getroottree().write(str(output_path))
