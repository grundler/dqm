
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
submit_dir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/batch'

#publish settings
default_publish_dir = '/tmp/tracktb/publish'

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

class RunStatus:
    def __init__(self, run, boards, datfiles, status=None):
        self.run = run
        self.boards = boards
        self.datfiles = datfiles
        if status is None:
            self.status = [ [STATUS.unknown]*JOBS.nJobs for board in boards ] 
        else:
            self.status = status

    def __str__(self):
        return "%d, %s, %s, %s" % (self.run, self.boards, self.datfiles, self.status)

    def readline(self, in_line):
        self.run = int(in_line.partition(',')[0])

        
