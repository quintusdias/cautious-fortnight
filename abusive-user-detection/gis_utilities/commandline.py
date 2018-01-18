# Standard library imports
import argparse
import datetime
import pathlib

from .ags_logs import DailySummary
from .ags_plot_stats import AGSServiceStatisticsPlotsViaMPL
from .ags_stats import CollectAgsStats, CollectAgsUsageRequests
from .ags_rest import AgsRestAdmin
from .akamai import RetrieveAkamaiLogs
from .big_brother import BBFlagHistory, ArcSocCounts
from .check_mk import CheckCheckMK
from .heatmap import HeatMap
from .daily_log_merge import DailyApacheLogCount, DailyApacheLogCountPlot
from .daily_log import ApacheLogBinner

BB_SITE_MAP = {
    'bldr': {
        'nowcoast': ('http://bb-bldr.ncep.noaa.gov'
                     '/IDP-Applications/IDP-NOWCOAST-OPS'
                     '/IDP-NOWCOAST-OPS.html'),
        'idpgis': ('http://bb-bldr.ncep.noaa.gov'
                   '/IDP-Applications/IDP-GIS-ESRI/IDP-GIS-ESRI.html'),
    },
    'cprk': {
        'nowcoast': ('http://bb.ncep.noaa.gov'
                     '/IDP-Applications/IDP-NOWCOAST-OPS'
                     '/IDP-NOWCOAST-OPS.html'),
        'idpgis': ('http://bb.ncep.noaa.gov'
                   '/IDP-Applications/IDP-GIS-ESRI/IDP-GIS-ESRI.html'),
    },
}


def valid_date_time(s):
    """
    Performs type-checking for entry point.
    """
    format = '%Y-%m-%dT%H'
    try:
        return datetime.datetime.strptime(s, format)
    except ValueError:
        msg = f"Not a valid date: '{s}.  Expecting '{format}'"
        raise argparse.ArgumentTypeError(msg)


def valid_date(s):
    """
    Performs type-checking for collect_ags_stats entry point.
    """
    try:
        return datetime.datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        msg = "Not a valid date: '{0}.".format(s)
        raise argparse.ArgumentTypeError(msg)


def bb_flag_history():
    """
    Entry point for command line script "bb_flag_hist"
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('site', choices=['cprk', 'bldr'])
    parser.add_argument('project', type=str)

    args = parser.parse_args()

    s = '/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common/nowcoast'
    root = pathlib.Path(s)
    dir = root / 'bigbrother' / 'summary' / args.site / args.project
    dir.mkdir(parents=True, exist_ok=True)
    path = dir / 'index.html'

    url = BB_SITE_MAP[args.site][args.project]
    obj = BBFlagHistory(url, output_file=str(path))
    obj.run()


def bin_daily_apache_log_file():
    """
    Console entry point for binning the daily apache log file by the minute.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('project',
                        choices=['idpgis', 'nowcoast'],
                        default='nowcoast')

    help = 'Path to input apache log file, hopefully you are just testing?'
    parser.add_argument('-i', '--input_file', type=str, help=help)

    help = 'Path to output HDF5 file, hopefully you are just testing?'
    parser.add_argument('-o', '--output_file', type=str, help=help)

    args = parser.parse_args()

    kwargs = {
        'project': args.project,
        'input_file': args.input_file,
        'output_file': args.output_file,
    }
    obj = ApacheLogBinner(**kwargs)
    obj.run()


def ccmk():
    """
    Query Check_MK for plots.
    """
    description = ("Command line utility for constructing a montage of plots "
                   "from Check_MK.")
    parser = argparse.ArgumentParser(description=description)

    help = ("Query Check_MK for these VMs.  This argument should be a "
            "fully-enumerated regular expression such as "
            "\"vm-bldr-gisapp-op[1,2][a,b]\"")
    parser.add_argument('--machines', help=help, nargs='*')

    help = "Query this Check_MK service."
    choices = [
        'CPU_load', 'CPU_utilization', 'Memory_used', 'fs__opt',
        'PostgreSQL_DB_postgres_Statistics:2'
    ]
    parser.add_argument('service', help=help, choices=choices)

    help = "Time range - YYYY-MM-DDTHH"
    parser.add_argument('--timerange', help=help, nargs=2,
                        type=valid_date_time,
                        required=True)

    help = "Output root."
    parser.add_argument('--output', help=help, type=str,
                        default=('/mnt/intra_wwwdev/ncep/ncepintradev/htdocs'
                                 '/ncep_common/nowcoast/check_mk'))

    args = parser.parse_args()

    obj = CheckCheckMK(args.machines, args.service, args.timerange,
                       args.output)
    obj.run()


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


def collect_arcsoc_counts():
    """
    Collect arcsoc counts each server.
    """
    description = "Command line utility for collecting arcsoc counts..."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('site',
                        choices=['CPRK', 'BLDR'],
                        help='Process this site''s arcsoc counts.')

    parser.add_argument('project',
                        choices=['idpgis', 'nowcoast'],
                        help='Process this project''s arcsoc counts.')

    args = parser.parse_args()

    obj = ArcSocCounts(args.site, args.project)
    obj.run()


def get_akamai_logs():
    """
    Retrieve akamai logs from remote FTP server.
    """
    description = ("Command line utility for retrieving Akamai web logs from"
                   "remote FTP server.")
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('project',
                        choices=['idpgis', 'nowcoast'],
                        help='Retrieve logs for this project.')
    args = parser.parse_args()

    obj = RetrieveAkamaiLogs(args.project)
    obj.run()


def count_nco_log_items():
    """
    Count the number of hits in the apache logs.
    """
    description = "Command line utility for counting hits in apache logs."
    parser = argparse.ArgumentParser(description=description)

    choices = ['idpgis', 'nowcoast', 'nowcoastqa']
    help = 'Process this project''s logs.'
    parser.add_argument('project', choices=choices, help=help)

    args = parser.parse_args()

    dest = pathlib.Path.home()
    dest = dest / 'data' / 'logs' / 'nco' / args.project / 'hits.csv'

    today = datetime.date.today()

    obj = DailyApacheLogCount(args.project, today, dest)
    obj.run()


def plot_nco_hits():
    """
    Plot the number of hits in the apache logs.
    """
    description = "Command line utility for plotting hits in apache logs."
    parser = argparse.ArgumentParser(description=description)

    choices = ['idpgis', 'nowcoast', 'nowcoastqa']
    help = 'Process this project''s logs.'
    parser.add_argument('project', choices=choices, help=help)

    args = parser.parse_args()

    src = pathlib.Path.home()
    src = src / 'data' / 'logs' / 'nco' / args.project / 'hits.csv'

    obj = DailyApacheLogCountPlot(args.project, src)
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

    if (((args.parameter == "status") and
         (args.value is not None) and
         (args.service is None))):
        msg = 'status requires service to be supplied'
        raise RuntimeError(msg)

    obj = AgsRestAdmin(args.site, args.project, args.tier, args.parameter,
                       server=args.server, service=args.service)
    obj.set_parameter(args.value)


def summarize_ags_logs():
    """
    Console script interface for summarizing the ags logs for a day.
    """
    formatter_class = argparse.RawTextHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=formatter_class)

    help = 'Project (need this to determine the services)'
    choices = ['idpgis', 'nowcoast']

    parser.add_argument('project', choices=choices, default='nowcoast',
                        help=help)

    parser.add_argument('site', choices=['cprk', 'bldr'], help='Site')

    choices = ["dev", "qa", "op"]
    parser.add_argument('tier', choices=choices, help='Tier')

    parser.add_argument('startdate',
                        help='The start date and hour - format YYYY-MM-DDTHH',
                        type=valid_date_time)

    parser.add_argument('stopdate',
                        help='The stop date and hour- format YYYY-MM-DDTHH',
                        type=valid_date_time)

    choices = ["SEVERE", "WARNING", "INFO", "FINE", "VERBOSE", "DEBUG", "OFF"]
    parser.add_argument('--level', choices=choices, default='SEVERE')

    help = (
        "Save dataframe results to file.  If the output filename\n"
        "ends in 'csv', then the output file is created as a CSV\n"
        "file.  If the output filename ends in 'pkl', then the\n"
        "output is 'pickled' and should be reloaded with\n"
        "\n"
        ">>> import pickle\n"
        ">>> with open(output_file, mode='rb') as f:\n"
        "...     df = pickle.load(f)"
    )
    parser.add_argument('--output', help=help)

    args = parser.parse_args()

    obj = DailySummary(args.project, args.site.upper(), args.tier,
                       args.startdate, args.stopdate, level=args.level,
                       output=args.output)
    obj.run()
