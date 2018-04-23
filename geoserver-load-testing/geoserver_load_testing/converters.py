"""
Converter functions for reading JMeter output.
"""
# Standard library imports
import datetime


def convert_bytes(r):
    """
    Convert bytes column.

    JMETER usually provides a "bytes" field which gives the length of
    the payload delivered by the HTTP request.

    The problem comes when HTTP requests fail, as the column values
    can get interchanged.  When that happens, the resulting value may
    not be convertible to integer.  By using this converter function,
    we can catch such instances and just give them a -1 value.

    Parameters
    ----------
    r : str
        Value from bytes column of JMETER results file.

    Returns
    -------
    bool
        Either True or False, the success or failure of the HTTP request.
    """
    try:
        return int(r)
    except ValueError:
        return -1


def convert_datatype(item):
    """
    Convert dataType field.

    JMETER usually provides a "dataType" field which is either "text" or "bin".
    For our cases, "bin" means an image response, which is a sign of success,
    "text" means a non-image response, which is bad.  By assigning 1 to "bin"
    and 0 to "text", we get an alternate check on success or failure.  This
    might be useful if we do not trust the success field (in the case of
    ArcGIS, we definitely do not trust it).

    Parameters
    ----------
    item : str
        Value from success column of JMETER results file.

    Returns
    -------
    int : Either 1 for "bin" or 0 for "text"
    """
    val = 1 if item == 'bin' else 0
    return val


def convert_success(item):
    """
    Convert success field.

    JMETER usually provides a "success" field which is either "true"
    or "false".  By specifying it as boolean, it can automatically be
    converted into True or False.

    The problem comes when HTTP requests fail, as the column values can
    get interchanged.  When that happens, the resulting value is often
    "text", which classifies as neither True nor False.  By using this
    converter function, we can catch such instances and classify them
    as False.

    Parameters
    ----------
    item : str
        Value from success column of JMETER results file.

    Returns
    -------
    bool
        Either True or False, the success or failure of the HTTP request.
    """
    if item == 'true':
        return True
    else:
        return False


def convert_response(r):
    """
    Provide integer reponse code.

    The responseCode value in a JMETER log file should be the typical
    HTTP 200 value.

    Sometimes, though, it is not.  It seems that when JMETER encounters
    a bad request, instead of a 500 or 404 or what-have-you, the
    reponse code column value is "Export map image with bounding box".
    This causes pandas to complain about mixed datatype columns, which
    is memory-intensive and slows things down.  Since all we really
    care about is success or failure, we will catch such exceptional
    rows and return -1 instead.

    Parameters
    ----------
    str
        Value from responseCode column of JMETER results file, usually
        '200'.

    Returns
    -------
    int
        Either 200 for success or -1 for a fail.
    """
    try:
        return int(r)
    except (TypeError, ValueError):
        return -1


def convert_timestamp(t):
    """
    Convert from milliseconds after the epoch to native datetime.

    Parameters
    ----------
    t : str
        Milliseconds after the epoch.

    Returns
    -------
    datetime.datetime
        Standard python datetime value.
    """
    try:
        ts = datetime.datetime.utcfromtimestamp(float(t) / 1000.0)
    except (OSError, ValueError, OverflowError) as e:
        print(repr(e))
        # Saw this once with a mangled line.
        #
        # 1508256351508273690379,1220,EXPORT,200,OK,
        # export wwa_meteoceanhydro_shortduration_hazards_warnings_time 8-1,
        # bin,true,3242,1,23,233
        #
        return None
    if ts.year > 2100:
        # Have seen one instance where a timestamp got corrupted.
        return None
    else:
        return ts


def convert_bool(t):
    try:
        return bool(t)
    except Exception as e:
        return False
