#!/bin/sh
# Usage: 
# acrontab -e 
# */5 * * * * lxplus.cern.ch /afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/dqm/fnal201403/bash/cron_jobs.sh

CMD="/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/dqm/fnal201403/python/dqm.py default"
LOGFILE="/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/www/dqm/fnal201403/dqm.log"

$CMD >> $LOGFILE 2>&1


