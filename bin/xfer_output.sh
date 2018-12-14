#!/bin/bash

# Transfer the summary graphics for the web logs to the intranet.

set -x
project=${1}

# rsync -avz "$project" -e ssh jevans@nco:/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common/nowcoast/analytics

remote_root=/usr2/ncep/ncepintradev/htdocs/ncep_common/nowcoast/analytics/"$project"
rsync -avz $project -e ssh jevans@vm-lnx-wwwdev1:"$remote_root"
