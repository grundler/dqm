#!/usr/bin/env python
"""
Main script DQM 

"""

__author__ = "Ulysses Grundler <grundler@cern.ch>"


import sys
import os 
#import shutil
#import subprocess
#import filecmp
import time     
import HTML
from datetime import datetime, timedelta 
import dqm
from helpers import *
import analyze

if hasattr(datetime, 'strptime'):
    strptime = datetime.strptime
else:
    strptime = lambda date_string, format: datetime(
        *(time.strptime(date_string, format)[0:6]))
# try:
#     import json
# except ImportError:
#     import simplejson as json

from Decoder_dqm import Decoder 

#Global variables
#dataset = 'FNAL2013'
#eosdir = 'eos/cms/store/cmst3/group/tracktb/'+dataset
# datadir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/data/'+dataset
# dbdir = datadir+'/.db/'
dbdir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/data/'+dataset+'/.db/'
#begin_valid_run = 37000
#end_valid_run = 50001
env_file = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/dqm/fnal201403/setup.sh'
#histdir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/jobsub/fnal201403/histograms'
mount_point = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/data'


#daqdir = '' #will be set when eos mounted

max_submissions = 5
#submission_script = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/jobsub/fnal201403/scripts/submitBatch.sh'

#Small class for organizing status information, status should proceed through each value as it goes along
class STATUS:
	  failed, unknown, submitted, returned, published, nStatus = range(-2,4)
	  prefix = ['submitted', 'returned', 'published', 'failed', 'unknown']
	  colors = ['aqua', 'teal' , 'green', 'red', 'white']

full_events = 999999999
short_events = 5000

#JOB = ('hits', 'tracks') # 'tracks') #prefix indicating what kind of job (conversion/clustering/hitmaker) or tracks, keep in order they need to be submitted
class JOBS:
    unknown, hits, tracks, nJobs = range(-1,3)
    prefix = ['hits', 'tracks', 'unknown']
    modes = [ ['convert', 'clustering', 'hitmaker'], ['tracks_prealign'], [] ]
    queues = ['1nh', '1nh', '']
    nevents = [full_events, short_events, 0]

debug = False 
#debug = True 


def main():
    args = sys.argv[1:]
    if len(args) == 0 :
        return usage()

    if ( len(args) == 1 and 
         args[0] == 'default' ):
        return default()

    if ( len(args) == 1 and 
         is_valid_run_str(args[0]) ):
        return default(args[0])

    function = getattr(dqm, args[0])
    return function(args[1:])


def usage():
    sys.stdout.write('''
NAME
    dqm.py (v3) 


SYNOPSIS
    dqm.py default 
           run the default procedure 

    dqm.py 30301
           only for run 30301

    dqm.py 30301-30350
           run the range between 30301-30350


AUTHOR
    Ulysses Grundler


REPORTING BUGS
    Report bugs to <grundler@cern.ch>.


DATE
    February 2014 

\n''')


def default(arg=None):
	#Start by mounting the eos directory, so we can do 'ls', 'ln -s', etc.
	mount_eos(mount_point)

	submissions = 0 #counter for how many jobs we've submitted

	sys.stdout.write('Getting runs\n')
	runs = get_runs()
	runs = sorted(runs, reverse=True)
	sys.stdout.write('Got %s runs\n' % len(runs))

	#loop over all the runs we found
	for run in runs:
		if submissions >= max_submissions:
		   break
		datfile = get_datfiles(run)
		if not datfile:
		   sys.stdout.write('No dat file for run %s.\n' %run) 
		   continue

		#Each run may have several dat files, loop over them.
		for dat in datfile:
			board = get_board(dat) 
			#should check to see if file is fully transfered.

			#check status of processing
			for job in range(JOBS.nJobs):
				status = get_job_status(job, dat)
				sys.stdout.write('Run: %s\tboard: %s\tjob: %s\tstatus: %s\n' %(run,board,JOBS.prefix[job],STATUS.prefix[status]))
				if status == STATUS.published:
				   sys.stdout.write('Nothing more to do for this job, moving on\n')
				   continue #continue to next job
				elif status == STATUS.returned:
					 sys.stdout.write('Job returned. Publishing\n')
					 publish(dat, job, run, board)
				elif status == STATUS.submitted:
					sys.stdout.write('Waiting for job to finish processing\n')
					break #can't go on with this run until this job is done
				else:
					#Need to submit the job
					exitcode = submit_job(job, run, dat)
					if 0==exitcode:
					    submissions += 1
					else:
						sys.stdout.write('Submission of %s job for run %s board %s failed. Please investigate\n' %(JOBS.prefix[job], run, board))
					break #if just submitted, can't go to the next job for this run

	#finish up
	umount_eos(mount_point)

	index(arg)
	sys.stdout.write('Submitted %s jobs\n' % submissions)

####

def get_job_status(job, filename):
	status = STATUS.unknown
	for st in range(STATUS.nStatus):
		#fullname = os.path.join(dbdir,'.'+STATUS.prefix[st]+'.'+JOBS.prefix[job]+'.'+file)
		fullname = db_file_name(filename, job, st)
		if os.path.isfile(fullname):
		    status = st
	return status
	
def submit_job(job, run, filename, test=False):
    if test:
        sys.stdout.write('Here we would submit %s job for run %s on %s file\n' % (JOBS.prefix[job], run, filename))
        #f = os.path.join(dbdir, '.'+STATUS.prefix[STATUS.submitted]+'.'+JOBS.prefix[job]+'.'+file)
        #open(f,'a').close()
        f = db_file_name(filename, job, STATUS.submitted, insert=True)

	#First, make sure no other process tries to submit
	# f = os.path.join(dbdir, '.'+STATUS.prefix[STATUS.submitted]+'.'+JOBS.prefix[job]+'.'+file)
	# open(f,'a').close()
    f = db_file_name(filename, job, STATUS.submitted, insert=True)

    fullpath = eosdir+"/"+str(run)+"/"+filename
	#Actually do the submission
    #give a file name to be created upon completion
    # f = os.path.join(dbdir, '.'+STATUS.prefix[STATUS.returned]+'.'+JOBS.prefix[job]+'.'+file)
    f = db_file_name(filename, job, STATUS.returned, insert=False)
    analyze.analyzeBatch([fullpath], JOBS.modes[job], 
                            dbfile=f, suffix='-'+JOBS.prefix[job],
                            queue=JOBS.queues[job], nevents=JOBS.nevents[job])
	#cmd = '%s %s' % (submission_script, run)
	#output = proc_cmd(cmd)
	#sys.stdout.write(output+'\n')

def publish(fname, job, run, board):                                                                                
    sys.stdout.write('[pub_dqm] run %s ... \n' % run)
    sys.stdout.flush()

    procenv = source_bash(env_file)
    #sys.stdout.write('procenv: %s\n' % procenv)
    histdir = os.path.join(mount_point, processed_dir, board, 'histograms')

    #Indicate we're published in the db
    db_file_name(fname, job, STATUS.published, insert=True)

    cmd = 'dqm %s %s' %(board, str(run).zfill(6))
    output = proc_cmd(cmd, procdir=histdir, env=procenv)
    print output
    sys.stdout.write(' OK.\n')
    
def index(arg): 
    sys.stdout.write('[make index] ... \n')
    sys.stdout.flush()
    procenv = source_bash(env_file)
    targetdir = procenv['TARGETDIRECTORY']

    sys.stdout.write('\tget db files\n')
    sys.stdout.flush()
    runs = []
    #data = []
    run_status = {}
    # cmd = 'ls %s' % dbdir
    # output = proc_cmd(cmd)
    # sys.stdout.write('\tgot db files\n')
    # sys.stdout.flush()
    # for line in output.split():
    dblist = os.listdir("%s" % dbdir)
    for line in dblist:
        # #submission is the first bit of info we have
        # if line.split('.')[1] is STATUS.prefix[STATUS.submitted]:
        run, board, job, status = parse_db(line)
        #sys.stdout.write('%s %s %s %s\n' % (run, board, job, status))
        #data.append((run,board,job,status))
        if run not in runs:
            runs.append(run)
            #run_status[run] = {}
            #run_status[run]['PixelTestBoard1'] = {}
            #run_status[run]['PixelTestBoard2'] = {}
        label = str(run).zfill(6)+'_'+board
        if (label) not in run_status:
            run_status[label] = {}
        #if job not in run_status[run][board] or run_status[run][board][job] < status:
        #   run_status[run][board][job] = status
        if job not in run_status[label] or run_status[label][job] < status:
            run_status[label][job] = status
        #sys.stdout.write('job %s run_status %s %s %s\n' % (JOBS.prefix[job], run_status, run_status[label], run_status[label][job]))

    runs = sorted(runs, reverse=True)

    header_row = ['Run']
    header_row.extend(JOBS.prefix[:-1])
    #header_row.extend(['PixelTestBoard1']*len(JOB))
    #header_row.extend(['PixelTestBoard2']*len(JOB))
    t = HTML.Table(header_row=header_row)
    #secondrow = ['']
    #for board in ['PixelTestBoard1', 'PixelTestBoard2']:
    #	for job in JOB:
    #		secondrow.append(job)
    #t.rows.append(secondrow)
    for run_board in sorted(run_status, reverse=True):
        run_link = HTML.link(run_board, '%s' %run_board)
        row = [run_link]

        for job in run_status[run_board]:
            color = STATUS.colors[run_status[run_board][job]]
            colored_result = HTML.TableCell(STATUS.prefix[run_status[run_board][job]], bgcolor=color)
            row.append(colored_result)
        t.rows.append(row)

    htmlcode = str(t)

    html_header = '''<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8" />
    <title>Test Beam DQM - clusters</title>
    <meta name="keywords" content="CERN CMS tracker upgrade" />
    <meta name="description" content="CMS Tracker upgrade summary page" />
    <link href=".style/default.css" rel="stylesheet" type="text/css" />
    <link rel="shortcut icon" type="image/x-icon" href=".style/images/favicon.ico">
 </head>
  <body>
      <div id="header">
    <h1>
    <a href="index.html">%s Test Beam DQM</a>
    </h1>
    </div>
    <div id="content">
    ''' % dataset 

    html_footer = '''<div id="footer">
    <p>Page created on %s </p>
    <p>&copy; <a href="mailto:Xin.Shi@cern.ch"> Xin Shi</a> 2013 </p>
    </div>
    </div>
    </body>
    </html>''' %  time.strftime("%Y-%m-%d %H:%M:%S GMT", time.gmtime())


    index = os.path.join(targetdir, 'index.html')
    #index = os.path.join(targetdir, 'index2.html')
    fo = open(index, 'w')
    fo.write(html_header)
    fo.write(htmlcode)
    fo.write(html_footer)
    fo.close()
    sys.stdout.write(' OK.\n')

def db_file_name(basename, job, status, insert=False):
    #f = os.path.join(dbdir, '.'+STATUS.prefix[status]+'.'+JOBS.prefix[job]+'.'+basename)
    f = os.path.join(dbdir, basename + '.' + JOBS.prefix[job] + '.' + STATUS.prefix[status])
    if insert:
        open(f, 'a').close()
    return f

def parse_db(dbfile):
    status = STATUS.unknown
    job = JOBS.unknown
    board = 'unknown'
    run = 0

    stem_suffix = dbfile.partition('.dat.')

    #get run and board
    run, board = parse_datfilename(stem_suffix[0])

    #Get job and status
    labels = (stem_suffix[2]).partition('.')
    for j in range(JOBS.nJobs):
        if labels[0] == JOBS.prefix[j]:
            job = j

    for s in range(STATUS.nStatus):
        if labels[2] == STATUS.prefix[s]:
            status = s
    

	# a = dbfile.lstrip('.') #remove leading '.'
	# b = a.split('.') #split rest of string up
	# #for s in b:
	# #	sys.stdout.write('%s\t' % s)

	# #Get status
	# st = b[0]
	# sys.stdout.write('st=%s\n' % st)
	# for i in range(STATUS.nStatus):
	# 	#sys.stdout.write('testing %s:%s\t' % (i, STATUS.prefix[i]))
	# 	if st == STATUS.prefix[i]:
	# 	   #sys.stdout.write('got it\t')
	# 	   status = i
	# #sys.stdout.write('\n')

	# #Get job
    # for j in range(JOBS.nJobs):
    #     if b[1] == JOBS.prefix[j]:
    #     job = j

	# #Get board
	# c = b[2].split('_spill_')
	# if 'PixelTestBoard' in c[0]:
	#    board = c[0]

	# #Get run
	# run = c[1].split('_')[0]
	# run = run.zfill(6)

    return run, board, job, status

####


if __name__ == '__main__':
    main()


