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

import logging
#LOGNAME = 'dqm-%s%s.log' % (os.getenv('HOSTNAME'), os.getenv('STY',''))
DATEFMT = '%H:%M:%S'
FORMAT = '%(asctime)s: %(name)s - %(levelname)s: %(message)s' 
logging.basicConfig(format=FORMAT, datefmt=DATEFMT, level=logging.DEBUG)
log = logging.getLogger("dqm")
# log.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s: %(name)s - %(levelname)s: %(message)s', '%H:%M:%S')
# ch.setFormatter(formatter)
# log.addHandler(ch)

max_submissions = 5
begin_valid_run = 175700
loop_back = 100

def main():
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-r", "--runs",
                        action="store",
                        dest="runs",
                        help="set runs to process, not yet operational")
    parser.add_option("-c", "--clear",
                        action="store_true",
                        dest="clear",
                        default=False,
                        help="Clear status of run defined using -r flag")
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
    elif options.clear:
        if not options.runs:
            parser.error('Need run number to clear')
        clear_status(options.runs)
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

    log.debug('Getting runs')
    # if debug:
    #     sys.stdout.write('Getting runs\n')
    runs = utils.get_runs(eos_mounted)
    runs = sorted(runs, reverse=True)
    log.info('Got %s runs', len(runs))
    # sys.stdout.write('Got %s runs\n' % len(runs))

    #loop over all the runs we found
    for run in runs:
        if run < begin_valid_run:
            break
        datfile = utils.get_datfile_names(run, eos_mounted)
        if not datfile:
            log.debug('No dat file for run %s', run)
            # if debug: 
            #     sys.stdout.write('No dat file for run %s.\n' %run) 
            continue

        #Each run may have several dat files, loop over them.
        for dat in datfile:
            board = utils.get_board(dat) 

            #check status of processing
            for job in range(JOBS.nJobs):
                status = utils.get_job_status(job, dat)
                log.debug('Run: %s\tboard: %s\tjob: %s\tstatus: %s' , run, board, JOBS.prefix[job], STATUS.prefix[status])
                # if debug:
                #     sys.stdout.write('Run: %s\tboard: %s\tjob: %s\tstatus: %s\n' %(run,board,JOBS.prefix[job],STATUS.prefix[status]))
                if status == STATUS.published:
                    # if debug: 
                    #     sys.stdout.write('Nothing more to do for this job, moving on\n')
                    continue #continue to next job
                elif status == STATUS.returned:
                    # if debug: 
                    #     sys.stdout.write('Job returned. Publishing\n')
                    publish_job(job, run, board, dat, eos_mounted)
                elif status == STATUS.submitted:
                    # if debug: 
                    #     sys.stdout.write('Waiting for job to finish processing\n')
                    break #can't go on with this run until this job is done
                elif submissions < max_submissions:
                    #Need to submit the job
                    process_job(job, dat, eos_mounted, batch)
                    submissions += 1
                    if batch:
                        break #if just submitted, can't go to the next job yet

    #finish up
    if eos_mounted:
        utils.umount_eos(eos_mount_point)

    index.index()
    log.info('Submitted %s jobs', submissions)
    # sys.stdout.write('Submitted %s jobs\n' % submissions)


def loop_processing(eos_mounted=False, batch=False):
    #Start by mounting the eos directory, so we can do 'ls', 'ln -s', etc.
    if eos_mounted:
        utils.mount_eos(eos_mount_point)

    submissions = 0 #counter for how many jobs we've submitted
    restart = False #restart at newewt runs after processing

    try:
        while True:
            restart = False
            log.info('Sleeping for just a second. Now would be a good time to interrupt')
            # sys.stdout.write('Sleeping for just a second. Now would be a good time to interrupt\n')
            # sys.stdout.flush()
            time.sleep(1)

            runs = utils.get_runs(eos_mounted)
            runs = sorted(runs, reverse=True)
            try:
                latest_run = runs[0]
            except IndexError:
                log.error('List index out of range?! No runs?! runs: %s', runs)

            log.info('Got %s runs', len(runs))
            # sys.stdout.write('Got %s runs\n' % len(runs))

            #loop over all the runs we found
            for run in runs:
                if restart:
                    break
                if run < latest_run - loop_back: #begin_valid_run:
                    break
                datfile = utils.get_datfile_names(run, eos_mounted)
                if not datfile:
                    log.debug('No dat file for run %s', run)
                    # if debug: 
                    #     sys.stdout.write('No dat file for run %s.\n' %run) 
                    continue

                #Each run may have several dat files, loop over them.
                for dat in datfile:
                    board = utils.get_board(dat) 

                    #check status of processing
                    for job in range(JOBS.nJobs):
                        status = utils.get_job_status(job, dat)
                        log.debug('Run: %s\tboard: %s\tjob: %s\tstatus: %s', run, board, JOBS.prefix[job], STATUS.prefix[status])
                        # if debug:
                        #     sys.stdout.write('Run: %s\tboard: %s\tjob: %s\tstatus: %s\n' %(run,board,JOBS.prefix[job],STATUS.prefix[status]))
                        if status == STATUS.submitted:
                            break #don't allow going to next job if not ready
                        elif status == STATUS.unknown:
                            process_job(job, dat, eos_mounted, batch)
                            submissions += 1
                            restart = True
                            if batch:
                                break

    except KeyboardInterrupt:
        log.warning('Processing Interrupted')
        # sys.stdout.write('ProcessingInterrupted\n')

    #finish up
    if eos_mounted:
        utils.umount_eos(eos_mount_point)

    log.info('Processed %s jobs', submissions)
    # sys.stdout.write('Submitted %s jobs\n' % submissions)

def loop_publishing(eos_mounted=False, batch=False):
    #Start by mounting the eos directory, so we can do 'ls', 'ln -s', etc.
    if eos_mounted:
        utils.mount_eos(eos_mount_point)

    submissions = 0 #counter for how many jobs we've submitted
    restart = 5 #restart at newest runs after publishing some runs

    try:
        while True:
            restart = 3
            log.info('Sleeping for just a second. Now would be a good time to interrupt')
            # sys.stdout.write('Sleeping for just a second. Now would be a good time to interrupt\n')
            # sys.stdout.flush()
            time.sleep(1)

            runs = utils.get_runs(eos_mounted)
            runs = sorted(runs, reverse=True)
            try:
                latest_run = runs[0]
            except IndexError:
                log.error('List index out of range?! No runs?! runs: %s', runs)

            log.info('Got %s runs', len(runs))
            # sys.stdout.write('Got %s runs\n' % len(runs))

            #loop over all the runs we found
            for run in runs:
                if restart == 0:
                    break
                if run < latest_run - loop_back: #begin_valid_run:
                    break
                datfile = utils.get_datfile_names(run, eos_mounted)
                if not datfile:
                    log.debug('No dat file for run %s', run)
                    # if debug: 
                    #     sys.stdout.write('No dat file for run %s.\n' %run) 
                    continue

                #Each run may have several dat files, loop over them.
                for dat in datfile:
                    board = utils.get_board(dat) 
        
                    #check status of processing
                    for job in range(JOBS.nJobs):
                        status = utils.get_job_status(job, dat)
                        log.debug('Run: %s\tboard: %s\tjob: %s\tstatus: %s', run, board, JOBS.prefix[job], STATUS.prefix[status])
                        # if debug:
                        #     sys.stdout.write('Run: %s\tboard: %s\tjob: %s\tstatus: %s\n' %(run,board,JOBS.prefix[job],STATUS.prefix[status]))
                        if status == STATUS.published:
                            # if debug: 
                            #     sys.stdout.write('Nothing more to do for this job, moving on\n')
                            continue #continue to next job
                        elif status == STATUS.returned:
                            # if debug: 
                            #     sys.stdout.write('Job returned. Publishing\n')
                            publish_job(job, run, board, dat, eos_mounted)
                            submissions += 1
                            restart -= 1
                        else:
                            break #if this job's not returned, other one can't be either

    except KeyboardInterrupt:
        log.warning('Publishing Interrupted')
        # sys.stdout.write('Publishing Interrupted\n')

    #finish up
    if eos_mounted:
        utils.umount_eos(eos_mount_point)

    log.info('Published %s jobs', submissions)
    # sys.stdout.write('Published %s jobs\n' % submissions)


####

def process_job(job, filename, eos_mounted=False, batch=False):
    log.info('process_job: %s - %s', filename, JOBS.prefix[job])
	#First, make sure no other process tries to submit
    f = utils.db_file_name(filename, job, STATUS.submitted, insert=True)

	#Actually do the submission
    #give a file name to be created upon completion
    f = utils.db_file_name(filename, job, STATUS.returned, insert=False)
    if not batch:
        process.process_dat(filename, JOBS.modes[job],
                            nevents=JOBS.nevents[job],
                            eos_mounted=eos_mounted, add_to_db=f)
    else:
        process.process_batch(filename, JOBS.modes[job],
                                nevents=JOBS.nevents[job],
                                eos_mounted=eos_mounted, add_to_db=f,
                                queue=JOBS.queues[job], suffix='-'+JOBS.prefix[job], script_dir=submit_dir)

    log.info('process_job finished: %s - %s', filename, job)

def publish_job(job, run, board, filename, eos_mounted=False):
    log.info('publish_job: Run %s %s - %s', str(run), board, job)
	#First, make sure no other process tries to submit
    f = utils.db_file_name(filename, job, STATUS.published, insert=True)

    publish.publish(run, board, eos_mounted=eos_mounted)
    log.info('publish_job finished: Run %s %s - %s', str(run), board, job)

def get_range_from_str(val, start=0, stop=None):

    def get_range_hypen(val):
        start = int(val.split('-')[0])
        tmp_stop = val.split('-')[1]
        if tmp_stop != '':
            stop = int(val.split('-')[1])+1
        return range(start, stop)

    result = []
    if '-' in val and ',' not in val:
        result = get_range_hypen(val)
        
    elif ',' in val:
        items = val.split(',')
        for item in items:
            if '-' in item:
                result.extend(get_range_hypen(item))
            else:
                result.append(int(item))
    else:
        result.append(int(val))

    #result = [ str(r).zfill(6) for r in result ]
    result.sort()
    log.debug('run list: %s', result)
    return result


def clear_status(run, boardname=None, jobname=None, statusname=None):
    log.debug('Clearing status for run %s', run)
    files = utils.get_datfile_names(run)

    for dat in files:
        #log.debug('dat: %s', dat)
        board = utils.get_board(dat) 
        if boardname is not None:
            if board != boardname:
                #log.debug('Do not touch %s', board)
                continue

        for job in range(JOBS.nJobs):
            if jobname is not None:
                if JOBS.prefix[job] != jobname:
                    #log.debug('Do not touch %s', JOBS.prefix[job])
                    continue
            
            for status in range(STATUS.nStatus):
                if statusname is not None:
                    if STATUS.prefix[status] != statusname:
                        #log.debug('Do not touch %s', STATUS.prefix[status])
                        continue

                f = utils.db_file_name(dat,job,status,remove=True)
                #log.debug('Removed %s', f)

####


if __name__ == '__main__':
    main()


