#!/bin/bash

# Transfer the summary graphics for the web logs to the intranet.

project=${1}

rsync -avz "$project" -e ssh jevans@nco:/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common/nowcoast/analytics
