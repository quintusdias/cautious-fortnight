# Standard library imports
import argparse
import datetime as dt

from .logs import SummarizeAgsLogs
from .plot_stats import AGSServiceStatisticsPlotsViaMPL
from .stats import CollectAgsStats, CollectAgsUsageRequests
from .rest import AgsRestAdmin
from .heatmap import HeatMap


class CustomTimeAction(argparse.Action):
    """
    Helper class for parsing sequence of ISO-formatted dates via command line.
    """
    def __init__(self,
                 option_strings,
                 dest,
                 nargs=None,
                 default=None,
                 type=None,
                 help=None):
        argparse.Action.__init__(self,
                                 option_strings=option_strings,
                                 dest=dest,
                                 nargs=nargs,
                                 default=default,
                                 type=type,
                                 help=help,
                                 )

    def __call__(self, parser, namespace, values, option_string=None):

        # Do some arbitrary processing of the input values
        values = [
            dt.datetime.strptime(item, '%Y-%m-%dT%H:%M:%S')
            for item in values
        ]
        # Save the results in the namespace using the destination
        # variable given to our constructor.
        setattr(namespace, self.dest, values)


def valid_date_time(s):
    """
    Performs type-checking for entry point.
    """
    format = '%Y-%m-%dT%H'
    try:
        return dt.datetime.strptime(s, format)
    except ValueError:
        msg = f"Not a valid date: '{s}.  Expecting '{format}'"
        raise argparse.ArgumentTypeError(msg)


def valid_date(s):
    """
    Performs type-checking for collect_ags_stats entry point.
    """
    try:
        return dt.datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        msg = "Not a valid date: '{0}.".format(s)
        raise argparse.ArgumentTypeError(msg)


def collect_ags_stats():
    """
    Collect AGS statistics from each server.
    """
    description = "Command line utility for collecting ArcGIS server stats.."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('site',
                        choices=['CPRK', 'BLDR'],
                        help='Process this site''s server stats.')

    parser.add_argument('project',
                        choices=['idpgisqa', 'idpgis', 'nowcoast'],
                        help='Process this project''s server stats.')

    parser.add_argument('priority',
                        choices=[1, 2],
                        type=int,
                        help='Process services with this priority.')

    args = parser.parse_args()

    obj = CollectAgsStats(args.site, args.project, args.priority)
    obj.run()


def get_ags_requests():
    """
    Command line entry point for CollectAgsUsageReports.  This is based off of

    http://server.arcgis.com
        /en/server/latest/administer/linux
        /example-create-a-report-of-all-service-requests.htm
    """
    description = "Command line utility for collecting AGS usage requests..."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('project',
                        choices=['idpgis', 'nowcoast'],
                        help='Process this project''s requests.')

    parser.add_argument('site',
                        choices=['cprk', 'bldr'],
                        help='Process this site''s requests.')

    parser.add_argument('tier',
                        choices=['dev', 'qa', 'op'],
                        help='Process this tier''s requests.')

    parser.add_argument('starttime',
                        help='The start date and hour - format YYYY-MM-DDTHH',
                        type=valid_date_time)

    parser.add_argument('num_hours',
                        help='The number of one hour intervals to poll.',
                        type=int)

    parser.add_argument('output', help='Save output to this file.')

    args = parser.parse_args()

    obj = CollectAgsUsageRequests(args.project, args.site, args.tier,
                                  args.starttime, args.num_hours, args.output)
    obj.run()


def heatmap():
    """
    Console script interface for generating a heat map from an apache log file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('input_file', type=str)
    parser.add_argument('output_file', type=str)
    parser.add_argument('ip_address', type=str)

    help = 'Project (need this to determine the services)'
    parser.add_argument('-p', '--project', choices=['idpgis', 'nowcoast'],
                        default='nowcoast', help=help)

    args = parser.parse_args()

    obj = HeatMap(args.input_file, args.output_file, args.ip_address,
                  project=args.project)
    obj.run()


def plot_mpl_ags_stats():
    """
    Console script interface for generating the AGS statistics plots via MPL.
    """
    parser = argparse.ArgumentParser()

    help = 'Physical site, either BLDR or CPRK.'
    parser.add_argument('site', choices=['CPRK', 'BLDR'], help=help)

    help = 'Project (need this to determine the services)'
    parser.add_argument('project', choices=['idpgis', 'idpgisqa', 'nowcoast'],
                        default='nowcoast', help=help)

    parser.add_argument('num_hours', type=int)

    args = parser.parse_args()

    obj = AGSServiceStatisticsPlotsViaMPL(args.site, args.project,
                                          args.num_hours)
    obj.run()


def set_ags():
    """
    Console script interface for setting/getting ArcGIS server parameters.
    """
    parser = argparse.ArgumentParser()

    choices = ["BLDR", "CPRK"]
    parser.add_argument('site', choices=choices)

    choices = ["idpgis", "nowcoast", "nowcoastqa"]
    parser.add_argument('project', choices=choices)

    choices = ["dev", "qa", "op"]
    parser.add_argument('tier', choices=choices)

    help = "Reset this parameter"
    choices = ["enableDynamicLayers",
               "maxInstancesPerNode", "minInstancesPerNode", "maxLogFileAge",
               "maxStartupTime", "recycleStartTime", "logLevel", "status"]
    parser.add_argument('parameter', choices=choices, help=help)

    help = "Parameter value, if not provided the current value is reported"
    parser.add_argument('--value', help=help)

    parser.add_argument('--server', type=str)
    parser.add_argument('--service', type=str)

    args = parser.parse_args()

    if (((args.parameter == "status")
         and (args.value is not None)
         and (args.service is None))):
        msg = 'status requires service to be supplied'
        raise RuntimeError(msg)

    obj = AgsRestAdmin(args.site, args.project, args.tier, args.parameter,
                       server=args.server, service=args.service)
    obj.set_parameter(args.value)


def summarize_ags_logs():
    """
    Console script interface for summarizing the ags logs.
    """
    formatter_class = argparse.RawTextHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter_class)

    help = 'Query this project.'
    choices = ['idpgis', 'nowcoast']
    parser.add_argument('project', choices=choices, help=help)

    help = 'Query this site.'
    parser.add_argument('site', choices=['cprk', 'bldr'], help=help)

    help = 'Query this tier.'
    choices = ["dev", "qa", "op"]
    parser.add_argument('tier', choices=choices, help='Tier')

    help = (
        "Save the output in this HDF5 file.  If no argument is provided, the "
        "output is written to "
        "/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common"
        "/nowcoast/ags_logs/$project/logs.h5"
    )
    parser.add_argument('--outfile', type=str, help=help)

    help = 'Time frame (default is prior 24 hours).'
    default_time = [
        dt.datetime.now() - dt.timedelta(hours=24), dt.datetime.now()
    ]
    parser.add_argument('--time', nargs=2, default=default_time, help=help,
                        action=CustomTimeAction)

    help = "Log items retrieved must have a log level at least this high."
    choices = ["SEVERE", "WARNING", "INFO", "FINE", "VERBOSE", "DEBUG", "OFF"]
    parser.add_argument('--level', choices=choices, default='WARNING')

    args = parser.parse_args()

    if args.outfile is None:
        outfile = (
            f"/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common"
            f"/nowcoast/ags_logs/{args.project}/logs.h5"
        )
    else:
        outfile = args.outfile

    obj = SummarizeAgsLogs(args.project, args.site, args.tier,
                           outfile, args.time, args.level)
    obj.run()
