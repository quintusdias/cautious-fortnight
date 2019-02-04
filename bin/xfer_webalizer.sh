#!/bin/bash

# rsync -avz webalizer -e ssh jevans@nco:/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common/nowcoast/

set -x
cd $HOME/www
remote_root=/usr2/ncep/ncepintradev/htdocs/ncep_common/nowcoast
rsync -avz webalizer -e ssh jevans@vm-lnx-wwwdev1:"$remote_root"
