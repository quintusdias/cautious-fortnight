#!/bin/bash

function filter_clean()
{
  # Get rid of the Akamai hostname
  sed -r 's#(http://|/origin.|/)?(idpgis|nowcoast).((bldr|cprk).)?ncep.noaa.gov(.akadns.net)?##g' | \
    # Restrict to a service request or layerinfo request.
    # 
    # Don't let the folder name be either "admin", "rest", or "system".
    #
    # Don't let the service name start with "mapserver".  Sometimes we also just see "mapserve"
    # make it easy and just restrict that.
    #
    # Don't let the service name be featureserver or gpserver or layers either.
    grep -P "(GET|POST) (?i)((/arcgis(/rest)?/services/(?!(admin|rest|system))\w+/((?!(mapserve|featureserver|gpserver|layers))\w+))|(/layerinfo))(?-i)" | \
      # Make it lowercase to make it easy.
      tr '[:upper:]' '[:lower:]' | \
        # Turn any sequences of '//' into just '/'
        sed 's#//*#/#g' | \
            # Filter out all but the request, status code, bytes, referrer, and user
            # agent.  Restrict the status code to 3-integer codes as a QA check.
	    #
	    # 000 : special Akamai code
	    # 100-103
	    # 200-229
	    # 300-310
	    # 400-429, 498, 499 : the last two are ESRI codes ?!!!
	    # 500-519
	    awk  '$9 ~ /^([1-5][0-2][0-9]|000|49[89])$/ {
	        print $7, $9, $10, $11, $12
	    }' 
}

function filter_ok_status() {
    # Akamai has a custom code of 000 for a terminated network connection that
    # we obviously must exclude from a "successful" count.
    #
    # Print out the request, bytes, referrer, and user agent.
    awk '{
        n = $2
        if (n >=200 && n < 400)
            print $1, $3, $4, $5
    }'
}

function filter_not_ok_status() {
    # Akamai has a custom code of 000 for a terminated network connection that
    # we obviously must include in an "error" count.
    #
    # Print out the request, bytes, referrer, and user agent.
    awk '{
        n = $2
        if (n <200 || n >= 400)
            print $1, $3, $4, $5
    }'
}

function filter_wmts()
{
    # /arcgis/rest/services/NOS_Biogeo_Biomapper/StJ_Imagery/MapServer/WMTS/1.0.0/WMTSCapabilities.xml
    #
    # /arcgis/services/folder/service/MapServer/WFSServer?request=GetCapabilities&service=WMTS
    egrep "/arcgis/rest/services/[[:alpha:]_]+/[[:alpha:]_]+/mapserver/wmts"
}

function pick_out_service()
{
    # Nowcoast has layerinfo, we don't want that.
    grep -v layerinfo | \
    # Reduce to "folder/service" bytes
    #
    # /arcgis/services/nws_observations/ahps_riv_gauges/mapserver/wmsserver 302 847
    awk '{
        split($1, path, /[/?]/)
        if ( path[3] == "services" ) {
            printf("%s/%s %s\n", path[4], path[5], $2)
        } else {
            # Presumably it is "rest"
            printf("%s/%s %s\n", path[5], path[6], $2)
        }
    }'
}

function pick_out_service_and_codes()
{
    # Nowcoast has layerinfo, we don't want that.
    grep -v layerinfo | \
    #
    # Reduce to "folder/service" status_code
    # Split on the URL path separator.
    #
    # e.g. input 
    #
    # /arcgis/services/nws_observations/ahps_riv_gauges/mapserver/wmsserver 302 847
    awk '{
        split($1, path, /[/?]/)
        if ( path[3] == "services" ) {
            printf("%s/%s %s\n", path[4], path[5], $2)
        } else {
            # Presumably it is "rest"
            printf("%s/%s %s\n", path[5], path[6], $2)
        }
    }'
}

function filter_wmts_getmap()
{
    # According to the docs, there should be a WMTS following the MapServer?
    #
    # /arcgis/rest/services/NOS_ESI/ESI_Shoreline_Carto/MapServer/tile/11/948/2046
    #
    #                  (?P<wmts>
    #                      # Regular WMTS URL
    #                      (
    #                        (/(layer=)?\d{1,})?
    #                        (/style=\d{1,})?
    #                        /tile
    #                        /(?P<tilematrix>\d{1,})
    #                        /(?P<tilerow>\d{1,})
    #                        /(?P<tilecol>\d{1,})
    #                      )
    #                    |
    #                      # KVP WMTS URL
    #                      /WMTS
    #                  )
    grep -P "mapserver/tile/\d+/\d+/\d+"
}

function filter_wfs_getfeature()
{
    # /arcgis/services
    # /NWS_Forecasts_Guidance_Warnings/watch_warn_adv/MapServer/WFSServer?
    # request=GetFeature&service=WFS&TypeName=watch_warn_adv:WatchesWarnings
    awk '$1 ~ /wfsserver.*request=getfeature/ {
        split($1, path, "?")
        printf("%s %s\n", path[1], $2)
    }'
}

function filter_export()
{
    awk '$1 ~ /(image|map)server\/export/ {
        split($1, path, "?")
        printf("%s %s\n", path[1], $2)
    }'
}

function filter_geoevent()
{
    grep -P geoevent
}

function filter_wms_getmap()
{
    #grep "wmsserver?.*request=getmap"
    awk '$1 ~ /wmsserver?.*request=getmap/ {
        split($1, path, "?")
        printf("%s %s\n", path[1], $2)
    }'
}

function sum_hits_codes()
{
    awk '{
        id = $1"|"$2

        # The service name is the first field, the number of 
        # bytes is the second field.
        #
        # Sum the hits.  The service name is the first field.
        hits[id] += 1
    } END {
        for (id in hits) {
            split(id, parts, /[|]/)
            svc = parts[1]
            status_code = parts[2]

            printf "%s %s %s\n", svc, status_code, hits[id]
        }
    }'
}

function sum_hits_bandwidth()
{
    awk '{
        # The service name is the first field, the number of 
        # bytes is the second field.
        #
        # Sum the hits.  The service name is the first field.
        hits[$1] += 1
             
        # Increment the bandwidth by the number of bytes
        # transferred in this transaction.
        bytes[$1] += $2
    } END {
        for (svc in hits) {
            printf "%s %s %s\n", svc, hits[svc], bytes[svc]
        }
    }'
}

function aggregate()
{
    pick_out_service | sum_hits_bandwidth | sort -nrk2,2
}

function aggregate_codes()
{
    pick_out_service_and_codes | sum_hits_codes | sort -nrk3,3
}

zcat ${1} | filter_clean | tee \
    >(filter_ok_status | tee \
        >(                        aggregate > p_all.dat) \
        >(filter_wms_getmap     | aggregate > p_wms.dat) \
        >(filter_export         | aggregate > p_export.dat) \
        >(filter_wfs_getfeature | aggregate > p_wfs_getfeature.dat) \
        >(filter_geoevent       | aggregate > p_geoevent.dat) \
        >(filter_wmts_getmap    | aggregate > p_wmts.dat) > /dev/null) \
    >(aggregate_codes                       > p_errors.dat) > /dev/null

join2html.py
