import argparse
from datetime import datetime

from .process_ingest_units import ProcessIngestUnits


def valid_date_time(s):                                                         
    format = '%Y-%m-%dT%H'                                                      
    try:                                                                        
        return datetime.strptime(s, format)                                     
    except ValueError:                                                          
        msg = f"Not a valid date: '{s}.  Expecting '{format}'"                  
        raise argparse.ArgumentTypeError(msg)                                   
 
def summarize_nc_ingest_units():
    """
    Entry point for utility plotting the nowcoast ingest units.
    """
    parser = argparse.ArgumentParser()

    help = "Search this directory for log files."
    parser.add_argument('dir', help=help)

    help = "Start time" 
    parser.add_argument('start', type=valid_date_time)

    help = "Stop time" 
    parser.add_argument('stop', type=valid_date_time)

    help = "Side" 
    parser.add_argument('--side', type=int)

    help = "Excluding these ingests" 
    parser.add_argument('--exclude', nargs='*')

    args = parser.parse_args()

    o = ProcessIngestUnits(args.dir, (args.start, args.stop), args.side, args.exclude)
    o.run()
