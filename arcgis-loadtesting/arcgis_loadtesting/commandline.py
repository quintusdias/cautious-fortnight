# Standard library imports
import argparse

# Local imports
from . import GenerateTestPlans, GenerateWMSinput, GenerateRESTinput
from . import RunLoadTest, Summarize


def summarize_arcgis_loadtest():
    """
    Entry point for bin script that summarizes CSV output files from a load
    test into a single Pandas dataframe.
    """
    description = (
        "Process all the JMeter CSV files for a GeoServer load test and "
        "reduce them to a single CSV file in long-form."
    )
    parser = argparse.ArgumentParser(description=description)

    help = ("YAML configuration file "
            "(same as used when generating the JMX files.")
    parser.add_argument('config', type=str, help=help)

    help = 'Write the output files in this directory.'
    parser.add_argument('--output', type=str, help=help, default='output')

    args = parser.parse_args()

    obj = Summarize(args.config, args.output)
    obj.run()


def generate_arcgis_rest_input():
    """
    Entry point for bin script that generates CSV files of input to feed into
    the REST endpoints for JMeter.
    """
    description = (
        "The input gzipped Apache log file probably should be preprocessed\n"
        "to restrict the requests being processed to just those associated\n"
        "with a particular service."
    )
    parser = argparse.ArgumentParser(description=description)

    help = "Gzipped Apache log file"
    parser.add_argument('input', help=help)

    help = "Output directory"
    parser.add_argument('output', help=help)

    args = parser.parse_args()

    obj = GenerateRESTinput(args.input, args.output)
    obj.run()


def generate_arcgis_wms_input():
    """
    Entry point for bin script that generates CSV files of WMS input for
    JMeter.
    """
    description = (
        "The input gzipped Apache log file probably should be preprocessed\n"
        "to restrict the requests being processed to just those associated\n"
        "with a particular service."
    )
    parser = argparse.ArgumentParser(description=description)

    help = "Gzipped Apache log file"
    parser.add_argument('input', help=help)

    help = "Output CSV file.  The separator will be '|'."
    parser.add_argument('output', help=help)

    args = parser.parse_args()

    obj = GenerateWMSinput(args.input, args.output)
    obj.run()


def run_arcgis_loadtest():
    """
    Entry point for bin script that runs a load test.
    """
    parser = argparse.ArgumentParser()

    help = "YAML configuration file"
    parser.add_argument('config', help=help)

    args = parser.parse_args()

    obj = RunLoadTest(args.config)
    obj.run()


def generate_arcgis_test_plans():
    """
    Entry point for bin script that generates JMeter test plans.
    """
    parser = argparse.ArgumentParser()

    help = "YAML configuration file"
    parser.add_argument('config', help=help)

    args = parser.parse_args()

    obj = GenerateTestPlans(args.config)
    obj.run()
