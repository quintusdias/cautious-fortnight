# Standard library imports ...
import argparse

# Third party library imports ...
import yaml

# Local imports ...
from . import RestToIso, NowCoastRestToIso
from .to_html import ISO191152_to_HTML


def rest2iso():

    description = 'Build ISO 19115-2 metadata from ArcGIS REST directory.'
    kwargs = {
        'description': description,
        'formatter_class': argparse.RawDescriptionHelpFormatter,
    }
    parser = argparse.ArgumentParser(**kwargs)

    help = 'YAML configuration file'
    parser.add_argument('config', type=str, help=help)

    help = 'Log level'
    choices = ['debug', 'info', 'warning', 'error', 'critical']
    parser.add_argument('--verbose', help=help, default='info',
                        choices=choices)

    args = parser.parse_args()

    # Get the project from the configuration file.
    with open(args.config, 'rt') as f:
        config = yaml.load(f.read())
    project = config['project']

    if project.lower() == 'nowcoast':
        obj = NowCoastRestToIso(args.config, verbose=args.verbose)
    else:
        obj = RestToIso(args.config, verbose=args.verbose)

    obj.run()


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
    parser.add_argument('output', type=str, help='Output root directory')

    args = parser.parse_args()

    o = ISO191152_to_HTML(args.input, args.output)
    o.run()
