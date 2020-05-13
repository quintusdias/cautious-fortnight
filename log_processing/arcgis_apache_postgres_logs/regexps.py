import re


# Regular expression for parsing apache combined log format.
pattern = r'''
    (?P<ip_address>[a-z\d.:]+)
    \s
    # Client identity, always -?
    -
    \s
    # Remote user, always -?
    -
    \s
    # Time of request.  The timezone is always UTC, so don't bother
    # parsing it.
    \[(?P<timestamp>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})\s.....\]
    \s
    # The request
    "(?P<request_op>(GET|DELETE|HEAD|OPTIONS|POST|PROPFIND|PUT))
    \s
    # match anything but a space
    (?P<path>[^ ]+)
    \s
    HTTP\/1.1"
    \s
    # Status code
    (?P<status_code>\d+)
    \s
    # payload size
    (?P<nbytes>\d+)
    \s
    # referer
    # Match anything but a double quote followed by a space
    "(?P<referer>.*?(?=" ))"
    \s
    # user agent
    # Match anything but a double quote.
    "(?P<user_agent>.*?)"
    \s
    # something else that seems to always be "-"
    "-"
    '''
apache_common_log_format_regex = re.compile(pattern, re.VERBOSE)

# Regular expression for matching an IDPGIS/nowCOAST URL path
pattern = r'''
           /(nowcoast|idpgis).ncep.noaa.gov.akadns.net
           /arcgis
           (?P<rest>/rest)?
           /services
           /(?P<folder>\w+)
           /(?P<service>\w+)
           /(?P<service_type>\w+)
           (
               /
               (
                   (
                       (?P<export>(export|exportimage))
                       (?P<export_mapdraws>.*?f=image)?
                   )
                   |
                   (
                       (?P<wms>wmsserver)
                       (?P<wms_mapdraws>.*?request=getmap)?
                   )
               )
           )?
           '''
path_regex = re.compile(pattern, re.VERBOSE | re.IGNORECASE)
