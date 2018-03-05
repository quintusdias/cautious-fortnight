"""
Define commonly used XPATH paths, other constant values
"""

# 3rd party library resources
import pkg_resources as pkg

# The WKT string contains the name of the coordinate system.  Use this to map
# to specific EPSG codes.
COORDINATE_SYSTEM_TO_EPSG = {
    'NAD_1983_StatePlane_Michigan_South_FIPS_2113_IntlFeet': 2253,
    'WGS_1984_Web_Mercator_Auxiliary_Sphere': 3857,
    # NCEP SPHERE should be 4053 in a perfect world?  Make it None until we
    # know for sure.
    'NCEP_SPHERE': None,
    'Spherical_Mercator': 3857,
    'World_Eckert_IV': 54012,
}

# Shortcuts for URLs of XML snippets already in docucomp.
_dc_prefix = "https://www.ngdc.noaa.gov/docucomp/"
DOCUCOMP = {
    3857: {
        'title': 'WGS 84 / Pseudo-Mercator',
        'url': _dc_prefix + "1fa4653d-4cf6-4d5f-b226-24a2f7ec31b1",
    },
    4269: {
        'title': 'North American Datum 1983',
        'url': _dc_prefix + "65f8b220-95ed-11e0-aa80-0800200c9a66",
    },
    4326: {
        'title': 'World Geodetic System 1984',
        'url': _dc_prefix + "2504d000-8345-11e1-b0c4-0800200c9a66",
    },
}

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:abstract',
    'gco:CharacterString'
]
ABSTRACT = '/'.join(parts)

AGGREGATION_INFO = '//gmd:aggregationInfo'

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:graphicOverview',
    'gmd:MD_BrowseGraphic',
    'gmd:fileName',
    'gco:CharacterString'
]
BROWSE_GRAPHIC_FILENAME = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:graphicOverview',
    'gmd:MD_BrowseGraphic',
    'gmd:fileType',
    'gco:CharacterString'
]
BROWSE_GRAPHIC_FILETYPE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:citation',
    'gmd:CI_Citation',
    'gmd:date',
    'gmd:CI_Date',
    'gmd:dateType',
    'gmd:CI_DateTypeCode[@codeListValue="creation"]',
    'ancestor::gmd:CI_Date',
    'gmd:date/gco:Date'
]
CREATION_DATE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:extent',
    'gmd:EX_Extent',
    'gmd:geographicElement',
    'gmd:EX_GeographicBoundingBox',
    'gmd:eastBoundLongitude',
    'gco:Decimal'
]
EAST_BOUNDING_LONGITUDE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:extent',
    'gmd:EX_Extent',
    'gmd:geographicElement',
    'gmd:EX_GeographicBoundingBox',
    'gmd:extentTypeCode',
    'gco:Boolean'
]
EXTENT_TYPE_CODE = '/'.join(parts)

parts = [
    'gmd:dateStamp',
    'gco:Date',
]
DATE_STAMP = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:descriptiveKeywords[not(@xlink:title)]',
]
DESCRIPTIVE_KEYWORDS = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:descriptiveKeywords[@xlink:title="GCMD Place"]',
]
DESCRIPTIVE_KEYWORDS__GCMD_PLACE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:descriptiveKeywords[@xlink:title="ISO Temporal"]',
]
DESCRIPTIVE_KEYWORDS__ISO_TEMPORAL = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:descriptiveKeywords[@xlink:title="NASA GCMD"]',
]
DESCRIPTIVE_KEYWORDS__NASA_GCMD = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:descriptiveKeywords[@xlink:title="WMO Theme"]'
]
DESCRIPTIVE_KEYWORDS__WMO_THEME = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:fileIdentifier',
    'gco:CharacterString',
]
FILE_IDENTIFIER = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:dataQualityInfo',
    'gmd:DQ_DataQuality',
    'gmd:lineage',
    'gmd:LI_Lineage',
    'gmd:statement',
    'gco:CharacterString',
]
LINEAGE_STATEMENT = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:dataQualityInfo',
    'gmd:DQ_DataQuality',
    'gmd:lineage',
    'gmd:LI_Lineage',
    'gmd:source',
    'gmi:LE_Source',
    'gmd:description',
    'gco:CharacterString',
]
LINEAGE_SOURCE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:dataQualityInfo',
    'gmd:DQ_DataQuality',
    'gmd:lineage',
    'gmd:LI_Lineage',
]
LI_LINEAGE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:dataQualityInfo',
    'gmd:DQ_DataQuality',
    'gmd:lineage',
    'gmd:LI_Lineage',
    'gmd:processStep',
]
PROCESS_STEP = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:extent',
    'gmd:EX_Extent',
    'gmd:geographicElement',
    'gmd:EX_GeographicBoundingBox',
    'gmd:northBoundLatitude',
    'gco:Decimal'
]
NORTH_BOUNDING_LATITUDE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:citation',
    'gmd:CI_Citation',
    'gmd:date',
    'gmd:CI_Date',
    'gmd:dateType',
    'gmd:CI_DateTypeCode[@codeListValue="publication"]',
    'ancestor::gmd:CI_Date',
    'gmd:date/gco:Date'
]
PUBLICATION_DATE = '/'.join(parts)

REFERENCE_SYSTEM_INFO = 'gmd:referenceSystemInfo'

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:containsOperations[@xlink:title="ArcGIS for Server REST Endpoint"]',
    'srv:SV_OperationMetadata',
    'srv:connectPoint',
    'gmd:CI_OnlineResource',
    'gmd:description',
    'gco:CharacterString',
]
REST_ENDPOINT_CONNECT_POINT_DESCRIPTION = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:containsOperations[@xlink:title="ArcGIS for Server REST Endpoint"]',
    'srv:SV_OperationMetadata',
    'srv:connectPoint',
    'gmd:CI_OnlineResource',
    'gmd:name',
    'gco:CharacterString',
]
REST_ENDPOINT_CONNECT_POINT_NAME = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:containsOperations[@xlink:title="ArcGIS for Server REST Endpoint"]',
    'srv:SV_OperationMetadata',
    'srv:connectPoint',
    'gmd:CI_OnlineResource',
    'gmd:linkage',
    'gmd:URL',
]
REST_ENDPOINT_CONNECT_POINT_URL = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:citation',
    'gmd:CI_Citation',
    'gmd:date',
    'gmd:CI_Date',
    'gmd:dateType',
    'gmd:CI_DateTypeCode[@codeListValue="revision"]',
    'ancestor::gmd:CI_Date',
    'gmd:date/gco:Date'
]
REVISION_DATE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:extent',
    'gmd:EX_Extent',
    'gmd:geographicElement',
    'gmd:EX_GeographicBoundingBox',
    'gmd:southBoundLatitude',
    'gco:Decimal'
]
SOUTH_BOUNDING_LATITUDE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:extent',
    'gmd:EX_Extent',
    'gmd:temporalElement',
    'gmd:EX_TemporalExtent',
    'gmd:extent',
    'gml:TimePeriod'
]
TIME_PERIOD = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'gmd:citation',
    'gmd:CI_Citation',
    'gmd:title',
    'gco:CharacterString'
]
TITLE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:distributionInfo',
    'gmd:MD_Distribution',
    'gmd:transferOptions',
    'gmd:MD_DigitalTransferOptions',
]
TRANSFER_OPTIONS = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:distributionInfo',
    'gmd:MD_Distribution',
    'gmd:transferOptions',
    'gmd:MD_DigitalTransferOptions',
    'gmd:onLine[@xlink:title="NOAA GeoPlatform Entry"]'
]
TRANSFER_OPTIONS__NOAA_GEOPLATFORM_ENTRY = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:extent',
    'gmd:EX_Extent',
    'gmd:geographicElement',
    'gmd:EX_GeographicBoundingBox',
    'gmd:westBoundLongitude',
    'gco:Decimal'
]
WEST_BOUNDING_LONGITUDE = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:containsOperations[@xlink:title="WMS Get Capabilities"]',
    'srv:SV_OperationMetadata',
    'srv:connectPoint',
    'gmd:CI_OnlineResource',
    'gmd:description',
    'gco:CharacterString',
]
WMS_CONNECT_POINT_DESCRIPTION = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:containsOperations[@xlink:title="WMS Get Capabilities"]',
    'srv:SV_OperationMetadata',
    'srv:connectPoint',
    'gmd:CI_OnlineResource',
    'gmd:name',
    'gco:CharacterString',
]
WMS_CONNECT_POINT_NAME = '/'.join(parts)

parts = [
    '/gmi:MI_Metadata',
    'gmd:identificationInfo',
    'srv:SV_ServiceIdentification',
    'srv:containsOperations[@xlink:title="WMS Get Capabilities"]',
    'srv:SV_OperationMetadata',
    'srv:connectPoint',
    'gmd:CI_OnlineResource',
    'gmd:linkage',
    'gmd:URL',
]
WMS_CONNECT_POINT_URL = '/'.join(parts)

# PROJ4 projection definitions that do not have a corresponding EPSG code.
PROJ4TEXT = {
    54012: '+proj=eck4',
    102003: ('+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 '
             '+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs')
}

UNKNOWN_WKTS = [
    ('GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6371200.0,0.0]],'
     'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'),
    ('GEOGCS["GCS_Sphere_EMEP",'
     'DATUM["D_Sphere_EMEP",SPHEROID["Sphere_EMEP",6371200.0,0.0]],'
     'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'),
]

input_file = pkg.resource_filename(__name__, "data/time_information.txt")
with open(input_file, mode='rt') as f:
    TIME_INFORMATION = f.read()
