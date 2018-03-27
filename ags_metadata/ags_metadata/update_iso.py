# Standard library imports
import pathlib
import warnings

# Third party library imports
from lxml import etree

# Local imports
from .rest2iso import NowCoastRestToIso, RestToIso


class UpdateIso(RestToIso):
    """
    """
    def __init__(self, config_file, input_root, output_root):
        pass


class NowCoastUpdateIso(NowCoastRestToIso):
    """
    """
    def __init__(self, config_file, input_root, output_root):
        super().__init__(config_file)

        self.input_root = pathlib.Path(input_root)
        if not self.input_root.exists():
            msg = (
                'The input ISO19115-2 XML directory {str(self.input_root)} '
                'does not exist.'
            )
            raise RuntimeError(msg)

        self.output_directory = pathlib.Path(output_root)


    def run(self):
        # Go thru the services listed in the config file
        if not self.session.verify:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                self._run()
        else:
            self._run()

    def _run(self):
        for folder in self.config['folder']:
            self.process_folder(folder['name'])

    def load_template(self):
        # Assumption is that a template file has been supplied.
        """
        The template is an XML file that has all the required elements present,
        but not filled out.
        """
        path = self.input_root / self.folder / (self.service + '.xml')
        self.tree = etree.parse(path.open(), self.parser)
        self.root = self.tree.getroot()

