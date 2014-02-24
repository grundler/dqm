#!/usr/bin/env python
"""
Main script DQM 

"""

__author__ = "Ulysses Grundler <grundler@cern.ch>"


import sys
import os 
import shutil
import subprocess
import filecmp
import time     
import HTML
from datetime import datetime, timedelta 
import dqm

if hasattr(datetime, 'strptime'):
    strptime = datetime.strptime
else:
    strptime = lambda date_string, format: datetime(
        *(time.strptime(date_string, format)[0:6]))
try:
    import json
except ImportError:
    import simplejson as json

from Decoder_dqm import Decoder 

#eos command
eos="/afs/cern.ch/project/eos/installation/cms/bin/eos.select"

#Global variables
dataset = 'FNAL2013'
eosdir = 'eos/cms/store/cmst3/group/tracktb/'+dataset
datadir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/data/'+dataset
dbdir = datadir+'/.db/'
begin_valid_run = 37000
end_valid_run = 50001

daqdir = '' #will be set when eos mounted

max_submissions = 5
submission_script = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/jobsub/fnal201403/scripts/submitBatch.sh'

STATUS = ['submitted', 'finished'] #prefix indicating job has been submitted or finished, keep in order of steps for process
JOB = ('hits', 'tracks') #prefix indicating what kind of job (conversion/clustering/hitmaker) or tracks, keep in order they need to be submitted

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
	mount_eos()

	submissions = 0 #counter for how many jobs we've submitted

	sys.stdout.write('Getting runs\n')
	runs = get_runs()
	runs = sorted(runs, reverse=True)
	sys.stdout.write('Got %s runs\n' % len(runs))

	#loop over all the runs we found
	for run in runs:
		if run == 35197:
		   continue
		if submissions >= max_submissions:
		   break
		datfile = get_datfile(run)
		if not datfile:
		   sys.stdout.write('No dat file for run %s.\n' %run) 
		   continue

		#Each run may have several dat files, loop over them.
		for dat in datfile:
			board = get_board(dat) 
			#should check to see if file is fully transfered.

			#check status of processing
			for job in JOB:
				status = get_job_status(job, dat)
				sys.stdout.write('For run %s, board %s: the %s job is %s\n' %(run,board,job,status))
				if status is STATUS[-1]:
				   sys.stdout.write('Nothing more to do for this job, moving on\n')
				   continue #continue to next job
				elif status in STATUS:
					sys.stdout.write('Waiting for job to finish processing\n')
					break #can't go on with this run until this job is done
				else:
					#Need to submit the job
					exitcode = submit_job(job, run, dat)
					if 0==exitcode:
					    submissions += 1
					else:
						sys.stdout.write('Submission of %s job for run %s board %s failed. Please investigate\n' %(job, run, board))
					break #if just submitted, can't go to the next job for this run

	#finish up
	umount_eos()

	sys.stdout.write('Submitted %s jobs\n' % submissions)

####

def get_job_status(job, file):
	status = 'unknown'
	for st in STATUS:
		fullname = os.path.join(dbdir,'.'+st+'.'+job+'.'+file)
		if os.path.isfile(fullname):
		    status = st
	return status
			
def get_runs():
	cmd = 'ls -1 %s' % daqdir
	output = proc_cmd(cmd)
	#sys.stdout.write('%s\n' % output)
	runs = get_runs_from_ls(output)
	return runs

def get_runs_from_ls(output):
    runs = []
    for line in output.split():
        if len(line) > 6: # skip non-valid run and files. 
            continue

        run = line.zfill(6)
        if not run.isdigit():
            continue
        
        run = int(run) 

        #if run < begin_valid_run or run > end_valid_run:
        #    continue

        if run not in runs:
            runs.append(run)

    return runs

def mount_eos():
	cmd = '%s -b fuse mount %s/eos' % (eos, datadir)
	output = proc_cmd(cmd)
	global daqdir
	daqdir = os.path.join(datadir, eosdir)
	#sys.stdout.write('datadir is %s\n' % datadir)
	#sys.stdout.write('eosdir is %s\n' % eosdir)
	#sys.stdout.write('daqdir is %s\n' % daqdir)

def umount_eos():
	cmd = '%s -b fuse umount %s/eos' % (eos, datadir)
	output = proc_cmd(cmd)
	global daqdir
	daqdir = ''

def get_datfile(run): 
    datfile = []
    cmd = 'ls -1 %s/%s' % (daqdir, run)
    output = proc_cmd(cmd)
    
    keyword = '.dat'
    for line in output.split():
        if keyword in line:
            f = os.path.join(daqdir, str(run), line)
            filesize = get_filesize(f) 
            if filesize > 10: 
                datfile.append(line)

    return datfile

def proc_cmd(cmd, test=False, verbose=1, procdir=None, env=os.environ):
    if test:
        sys.stdout.write(cmd+'\n')
        return 

    cwd = os.getcwd()
    if procdir != None:
        os.chdir(procdir)

    process = subprocess.Popen(cmd.split(), 
                               stdout=subprocess.PIPE, env=env)
    process.wait()
    stdout = process.communicate()[0]
    if 'error' in stdout:
        sys.stdout.write(stdout)
    if procdir != None:
        os.chdir(cwd)
    return stdout

def get_board(dat): 
    board = None 
    name = dat.split('_')[0]
    if 'PixelTestBoard' in name: 
        board = name 
    return board 

def ln_dat(run, board, dat, force=False):
	dstdir = get_rundir(run, board) 
	if os.path.exists(dstdir) and not force: 
	   sys.stdout.write('Skip linking %s_%s.\n' %( run, board))
	   return 

	cmd = "mkdir -p %s; cd %s" %(dstdir,dstdir) 
	proc_cmd(cmd)

	srcfile = os.path.join(datadir, eosdir, str(run), dat)
	cmd = 'ln -s %s .; cd -' %(srcfile)
	output = proc_cmd(cmd)

def submit_job(job, run, file, test=False):
	code = 0
	if test:
	   sys.stdout.write('Here we submit %s job for run %s on %s file\n' % (job, run, file))
	   f = os.path.join(dbdir, '.'+STATUS[0]+'.'+job+'.'+file)
	   open(f,'a').close()
	   return code

	#First, make sure no other process tries to submit
	f = os.path.join(dbdir, '.'+STATUS[0]+'.'+job+'.'+file)
	open(f,'a').close()

	#Actually do the submission
	cmd = '%s %s' % (submission_script, run)
	output = proc_cmd(cmd)
	sys.stdout.write(output+'\n')

	return code

def get_filesize(f):
    cmd = 'ls -l %s' % f
    output = proc_cmd(cmd)
    items = output.split()
    size = items[4]
    if not size.isdigit(): 
        sys.stdout.write('WARNING: not able to get file size \n')
        raise NameError(output)
    size = int(size)
    return size 

####


if __name__ == '__main__':
    main()


