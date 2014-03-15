#!/usr/bin/env python
"""
Main script DQM 

"""

__author__ = "Ulysses Grundler <grundler@cern.ch>"


import sys
import os 
from optparse import OptionParser
import time     
#import HTML
#from datetime import datetime, timedelta 
#import dqm

from config import *
import utils
import process
import publish
import index


max_submissions = 5
begin_valid_run = 172000

def main():
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-r", "--runs",
                        action="store",
                        dest="runs",
                        help="set runs to process, not yet operational")
    # parser.add_option("-b", "--board",
    #                     action="store",
    #                     dest="board",
    #                     help="Set board for run")
    parser.add_option("-e", "--eos_mounted",
                        action="store_true",
                        dest="eos_mounted",
                        default=False,
                        help="run on mounted EOS directory")
    parser.add_option("-b", "--batch",
                        action="store_true",
                        dest="batch",
                        default=False,
                        help="Set to run on batch queue")
    parser.add_option("-s", "--processloop",
                        action="store_true",
                        dest="processloop",
                        default=False,
                        help="Just loop processing")
    parser.add_option("-p", "--publishloop",
                        action="store_true",
                        dest="publishloop",
                        default=False,
                        help="Just loop publishing")
    (options, args) = parser.parse_args()


    if ( len(args) == 1 and
        args[0] == 'default' ):
        default(eos_mounted=False, batch=True)
    elif options.processloop:
        loop_processing(options.eos_mounted, options.batch)
    elif options.publishloop:
        loop_publishing(options.eos_mounted, options.batch)
    else:
        default(options.eos_mounted, options.batch)


def default(eos_mounted=False, batch=False):
    #Start by mounting the eos directory, so we can do 'ls', 'ln -s', etc.
    if eos_mounted:
        utils.mount_eos(eos_mount_point)

    submissions = 0 #counter for how many jobs we've submitted

    if debug:
        sys.stdout.write('Getting runs\n')
    runs = utils.get_runs(eos_mounted)
    runs = sorted(runs, reverse=True)
    sys.stdout.write('Got %s runs\n' % len(runs))

    #loop over all the runs we found
    for run in runs:
        if run < begin_valid_run:
            break
        datfile = utils.get_datfile_names(run, eos_mounted)
        if not datfile:
            if debug: 
                sys.stdout.write('No dat file for run %s.\n' %run) 
            continue

        #Each run may have several dat files, loop over them.
        for dat in datfile:
            board = utils.get_board(dat) 

            #check status of processing
            for job in range(JOBS.nJobs):
                status = get_job_status(job, dat)
                if debug:
                    sys.stdout.write('Run: %s\tboard: %s\tjob: %s\tstatus: %s\n' %(run,board,JOBS.prefix[job],STATUS.prefix[status]))
                if status == STATUS.published:
                    if debug: 
                        sys.stdout.write('Nothing more to do for this job, moving on\n')
                    continue #continue to next job
                elif status == STATUS.returned:
                    if debug: 
                        sys.stdout.write('Job returned. Publishing\n')
                    publish_job(job, run, board, dat, eos_mounted)
                elif status == STATUS.submitted:
                    if debug: 
                        sys.stdout.write('Waiting for job to finish processing\n')
                    break #can't go on with this run until this job is done
                elif submissions < max_submissions:
                    #Need to submit the job
                    process_job(job, run, dat, eos_mounted, batch)
                    submissions += 1
                    if batch:
                        break #if just submitted, can't go to the next job yet

    #finish up
    if eos_mounted:
        utils.umount_eos(eos_mount_point)

    index.index()
    sys.stdout.write('Submitted %s jobs\n' % submissions)


def loop_processing(eos_mounted=False, batch=False):
    #Start by mounting the eos directory, so we can do 'ls', 'ln -s', etc.
    if eos_mounted:
        utils.mount_eos(eos_mount_point)

    submissions = 0 #counter for how many jobs we've submitted
    restart = False #restart at newewt runs after processing

    try:
        while True:
            restart = False
            sys.stdout.write('Sleeping for just a second. Now would be a good time to interrupt\n')
            sys.stdout.flush()
            time.sleep(1)

            if debug:
                sys.stdout.write('Getting runs\n')
            runs = utils.get_runs(eos_mounted)
            runs = sorted(runs, reverse=True)
            sys.stdout.write('Got %s runs\n' % len(runs))

            #loop over all the runs we found
            for run in runs:
                if restart:
                    break
                if run < begin_valid_run:
                    break
                datfile = utils.get_datfile_names(run, eos_mounted)
                if not datfile:
                    if debug: 
                        sys.stdout.write('No dat file for run %s.\n' %run) 
                    continue

                #Each run may have several dat files, loop over them.
                for dat in datfile:
                    board = utils.get_board(dat) 

                    #check status of processing
                    for job in range(JOBS.nJobs):
                        status = get_job_status(job, dat)
                        if debug:
                            sys.stdout.write('Run: %s\tboard: %s\tjob: %s\tstatus: %s\n' %(run,board,JOBS.prefix[job],STATUS.prefix[status]))
                        if status == STATUS.submitted:
                            break #don't allow going to next job if not ready
                        elif status == STATUS.unknown:
                            process_job(job, run, dat, eos_mounted, batch)
                            submissions += 1
                            restart = True
                            if batch:
                                break

    except KeyboardInterrupt:
        sys.stdout.write('ProcessingInterrupted\n')

    #finish up
    if eos_mounted:
        utils.umount_eos(eos_mount_point)

    sys.stdout.write('Submitted %s jobs\n' % submissions)

def loop_publishing(eos_mounted=False, batch=False):
    #Start by mounting the eos directory, so we can do 'ls', 'ln -s', etc.
    if eos_mounted:
        utils.mount_eos(eos_mount_point)

    submissions = 0 #counter for how many jobs we've submitted
    restart = 5 #restart at newest runs after publishing some runs

    try:
        while True:
            restart = 5
            sys.stdout.write('Sleeping for just a second. Now would be a good time to interrupt\n')
            sys.stdout.flush()
            time.sleep(1)

            if debug:
                sys.stdout.write('Getting runs\n')
            runs = utils.get_runs(eos_mounted)
            runs = sorted(runs, reverse=True)
            sys.stdout.write('Got %s runs\n' % len(runs))

            #loop over all the runs we found
            for run in runs:
                if restart == 0:
                    break
                if run < begin_valid_run:
                    break
                datfile = utils.get_datfile_names(run, eos_mounted)
                if not datfile:
                    if debug: 
                        sys.stdout.write('No dat file for run %s.\n' %run) 
                    continue

                #Each run may have several dat files, loop over them.
                for dat in datfile:
                    board = utils.get_board(dat) 
        
                    #check status of processing
                    for job in range(JOBS.nJobs):
                        status = get_job_status(job, dat)
                        if debug:
                            sys.stdout.write('Run: %s\tboard: %s\tjob: %s\tstatus: %s\n' %(run,board,JOBS.prefix[job],STATUS.prefix[status]))
                        if status == STATUS.published:
                            if debug: 
                                sys.stdout.write('Nothing more to do for this job, moving on\n')
                            continue #continue to next job
                        elif status == STATUS.returned:
                            if debug: 
                                sys.stdout.write('Job returned. Publishing\n')
                            publish_job(job, run, board, dat, eos_mounted)
                            submissions += 1
                            restart -= 1
                        else:
                            break #if this job's not returned, other one can't be either

    except KeyboardInterrupt:
        sys.stdout.write('Publishing Interrupted\n')

    #finish up
    if eos_mounted:
        utils.umount_eos(eos_mount_point)

    sys.stdout.write('Published %s jobs\n' % submissions)


####

def get_job_status(job, filename):
	status = STATUS.unknown
	for st in range(STATUS.nStatus):
		fullname = utils.db_file_name(filename, job, st)
		if os.path.isfile(fullname):
		    status = st
	return status
	
def process_job(job, run, filename, eos_mounted=False, batch=False):
	#First, make sure no other process tries to submit
    f = utils.db_file_name(filename, job, STATUS.submitted, insert=True)

	#Actually do the submission
    #give a file name to be created upon completion
    f = utils.db_file_name(filename, job, STATUS.returned, insert=False)
    if not batch:
        process.process_run(run, JOBS.modes[job],
                            nevents=JOBS.nevents[job],
                            eos_mounted=eos_mounted, add_to_db=f)
    else:
        process.process_batch(run, JOBS.modes[job],
                                nevents=JOBS.nevents[job],
                                eos_mounted=eos_mounted, add_to_db=f,
                                queue=JOBS.queues[job], suffix='-'+JOBS.prefix[job], script_dir=submit_dir)

def publish_job(job, run, board, filename, eos_mounted=False):
	#First, make sure no other process tries to submit
    f = utils.db_file_name(filename, job, STATUS.published, insert=True)

    publish.publish(run, board, eos_mounted=eos_mounted)

####


if __name__ == '__main__':
    main()


