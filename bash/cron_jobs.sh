#!/bin/sh
# Usage: 
# acrontab -e 
# */5 * * * * lxplus.cern.ch /afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/dqm/fnal201403/bash/cron_jobs.sh

/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/dqm/fnal201403/python/dqm.py default 

#/afs/cern.ch/work/x/xshi/public/dqm/v2/python/ful.py

#/afs/cern.ch/work/x/xshi/public/dqm/v2/python/chk_data_integrity 

