
debug = False

#Global variables specific to testbeam setup
dataset='FNAL2014'
dbdir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/data/'+dataset+'/.db/'
dqm_env_file = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/dqm/fnal201403/setup.sh'

#eos command
eos="/afs/cern.ch/project/eos/installation/cms/bin/eos.select"

#EOS settings
eos_mount_point = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/data'
eosdir = 'eos/cms/store/cmst3/group/tracktb/'+dataset
processed_dir = eosdir+ '/processed'
daqdir='/'+eosdir
tprefix = '.transferred.'

#cmspxltb-analysis settings
job_config_file = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/jobsub/fnal201403/batch.cfg'
default_work_dir = '/tmp/tracktb/working'
analysis_env_file = '/afs/cern.ch/user/g/grundler/work/public/fnal2013/cmspxltb-analysis/build_env.sh'


#Small class for organizing status information, status should proceed through each value as it goes along
class STATUS:
	  failed, unknown, submitted, returned, published, nStatus = range(-2,4)
	  prefix = ['submitted', 'returned', 'published', 'failed', 'unknown']
	  colors = ['aqua', 'teal' , 'green', 'red', 'white']

full_events = 999999999
short_events = 5000

#Small class for organizing job types
class JOBS:
    unknown, hits, tracks, nJobs = range(-1,3)
    prefix = ['hits', 'tracks', 'unknown']
    modes = [ ['convert', 'clustering', 'hitmaker'], ['tracks_prealign'], [] ]
    queues = ['1nh', '1nh', '']
    nevents = [full_events, full_events, 0]
