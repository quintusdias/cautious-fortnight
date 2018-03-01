#!/bin/bash

cd $HOME/www
rsync -avz webalizer -e ssh jevans@nco:/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common/nowcoast/
