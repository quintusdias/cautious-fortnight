# Standard library imports ...
import argparse
import pathlib

# Third party library imports ...
import yaml

# Local imports ...
from . import RestToIso, NowCoastRestToIso
from .to_html import ISO191152_to_HTML
from .update_iso import NowCoastUpdateIso, UpdateIso


def update_iso():
    """
    Update an existing ISO record against REST metadata and a configuration
    file.
    """
    description = 'Update ISO 19115-2 metadata'
    kwargs = {
        'description': description,
        'formatter_class': argparse.RawDescriptionHelpFormatter,
    }
    parser = argparse.ArgumentParser(**kwargs)

    help = 'YAML configuration file'
    parser.add_argument('config', type=str, help=help)

    help = 'Root directory of existing XML records'
    parser.add_argument('input', type=str, help=help)

    help = 'Output directory of updated XML records'
    parser.add_argument('output', type=str, help=help)

    args = parser.parse_args()

    # Get the project from the configuration file.
    with open(args.config, 'rt') as f:
        config = yaml.load(f.read())

    if config['project'].lower() == 'nowcoast':
        obj = NowCoastUpdateIso(args.config, args.input, args.output)
    else:
        obj = UpdateIso(args.config, args.input, args.output)

    obj.run()

def rest2iso():

    description = 'Build ISO 19115-2 metadata from ArcGIS REST directory.'
    parser = argparse.ArgumentParser(description=description)

    help = 'YAML configuration file'
    parser.add_argument('config', type=str, help=help)

    help = 'Log level'
    choices = ['debug', 'info', 'warning', 'error', 'critical']
    parser.add_argument('--verbose', help=help, default='info',
                        choices=choices)

    help = (
        'Output directory (default is the current directory).  Underneath '
        'this directory, a subdirectory corresponding to the server name '
        'will be created (see the config file for that), and under that, '
        '\'xml\' and \'html\' directories will be created.  And under THOSE, '
        'the service folders are created, which house the XML documents and '
        'their HTML counterparts.'
    )
    parser.add_argument('--output', help=help, default=pathlib.Path.cwd())

    args = parser.parse_args()

    # Get the project from the configuration file.
    with open(args.config, 'rt') as f:
        config = yaml.load(f.read(), Loader=yaml.FullLoader)
    project = config['project']

    if project.lower() == 'nowcoast':
        klass = NowCoastRestToIso
    else:
        klass = RestToIso

    rest2iso_obj = klass(args.config, args.output, verbose=args.verbose)
    rest2iso_obj.run()

    input_dir = rest2iso_obj.output_directory
    output_dir = input_dir.parents[0] / 'html'
    o = ISO191152_to_HTML(input_dir, output_dir, logger=rest2iso_obj.logger)
    o.run()

def iso191152_to_html():
    """
    Entry point for converting ISO19115-2 documents to HTML.
    """
    kwargs = {
        'description': 'Convert ISO 19115-2 XML documents into HTML.',
        'formatter_class': argparse.RawDescriptionHelpFormatter,
    }
    parser = argparse.ArgumentParser(**kwargs)

    parser.add_argument('input', type=str, help='Input root directory')

    help = 'Output directory (default is the current directory).'
    parser.add_argument('--output', help=help, default=pathlib.Path.cwd())

    args = parser.parse_args()

    o = ISO191152_to_HTML(args.input, args.output)
    o.run()
