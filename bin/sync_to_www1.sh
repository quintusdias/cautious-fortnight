#!/bin/bash
#
# Sync all ~/www content to vm-lnx-wwwdev1
remote_root=/usr2/ncep/ncepintradev/htdocs/ncep_common/nowcoast
rsync -avz --exclude 'ags_logs/*/*.h5' $HOME/www/ jevans@vm-lnx-wwwdev1:$remote_root
