#!/usr/bin/env python
"""
Analyze a run (do any or all of convert, clustering, hitmaker, align, tracks)
   and send the output to a central location
Primarily intended to be submitted to batch queues, but can be run locally

"""

__author__ = "Ulysses Grundler <grundler@cern.ch>"

import sys
import os
from datetime import datetime
from optparse import OptionParser
from helpers import *

#define full set of modes
allmodes =['convert', 'clustering', 'hitmaker', 'align', 'tracks']

#Job configuration info
job_config_file = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/jobsub/fnal201403/batch.cfg'

#build environment to run in
env_file = '/afs/cern.ch/user/g/grundler/work/public/fnal2013/cmspxltb-analysis/build_env.sh'

#working directories
work_dir = '/tmp/tracktb/batchsub'
eos_mount_point = '/tmp/tracktb'
submit_dir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/batch'

def main():
    parser = OptionParser(usage="usage: %prog [options] filename")
    parser.add_option("-m", "--modes",
                        action="append",
                        dest="modes",
                        help="Set modes to run")
    parser.add_option("-q", "--queue",
                        action="store",
                        dest="queue",
                        default='1nh',
                        help="Set batch queue to submit to (only for batch)")
    parser.add_option("-d", "--dbfile",
                        action="store",
                        dest="dbfile",
                        default=None,
                        help="Name a db file to create upon completion")
    parser.add_option("-n", "--nevents",
                        action="store",
                        dest="nevents",
                        default=999999999,
                        help="Number of events to process")    
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("Too few arguments")

    if options.modes is None:
        modes = allmodes
    else:
        modes = options.modes

    if len(args) == 1:
        analyze(args[0], modes, dbfile=options.dbfile, nevents=int(options.nevents))
    else:
        analyzeBatch(args, modes, dbfile=options.dbfile, queue=options.queue, nevents=int(options.nevents))

# eos_file should be full path of file in eos starting with 'eos/' not '/eos/'
def analyze(eos_file, modes, run=0, board='unknown', dbfile=None, nevents=999999999):
    # cmd = 'env'
    # output = proc_cmd(cmd)
    # sys.stdout.write('env %s\n' % output)
    # sys.stdout.flush()

    sys.stdout.write('Start analysis of %s:\n\tmodes - %s\n' % (eos_file, modes))
    sys.stdout.flush()

	#source environment for running
    procenv = source_bash(env_file)
    # sys.stdout.write('procenv: %s\n' % procenv)
    # sys.stdout.flush()

    #Find run and board information
    if run == 0 or board == 'unknown':
        r, b = parse_datfilename(eos_file)
    if run == 0:
        run = r
    if board == 'unknown':
        board = b
    sys.stdout.write('\trun: %s, board: %s\n' % (run, board))
    sys.stdout.flush()

    #create a unique string to keep eos mounts separate so they don't disappear when running multiple jobs
    mytime = str(datetime.today()).split(' ')[1].replace(':','').replace('.','')
    mount_point = eos_mount_point+mytime
    workdir = work_dir+mytime

    sys.stdout.write('\tMount eos at: %s\n' % mount_point)
    sys.stdout.write('\tWork from: %s\n' % workdir)

	#Mount EOS and link data from EOS
    rundir = os.path.join(workdir,'data','cmspixel',str(run).zfill(6))
    cmd = 'mkdir -p %s' % rundir
    output = proc_cmd(cmd)
    # sys.stdout.write('%s\n' % output)
    # sys.stdout.flush()

    mount_eos(mount_point)
    cmd = 'ln -sf %s mtb.bin' % os.path.join(mount_point,eos_file)
    output = proc_cmd(cmd,procdir=rundir)
    # sys.stdout.write('%s\n' % output)
    # sys.stdout.flush()
    if not os.path.exists(os.readlink(os.path.join(rundir,'mtb.bin'))):
        sys.stdout.write('mtb.bin is broken symlink\n')

    #Create modified copy of config file, appropriate to the board
    cfg_file = modified_copy(job_config_file, workdir, board, nevents, mount_point=mount_point)

	#actually run
    for mode in modes:
        sys.stdout.write('\nProcessing: jobsub -c %s %s %s\n' % (cfg_file, mode, run))
        sys.stdout.flush()

        cmd = 'jobsub -c %s %s %s' % (cfg_file, mode, run)
        output = proc_cmd(cmd, procdir=workdir, env=procenv)

        sys.stdout.write('OK\n')

    #If requested, reate a file indicating this is completed
    if dbfile is not None:
        open(dbfile,'a').close()

	#clean up
    sys.stdout.write('Done processing. Time to clean up\n')
    sys.stdout.flush()
    umount_eos(mount_point)
    cmd = 'rm -r %s' % workdir
    proc_cmd(cmd)

    sys.stdout.write('End analysis of %s: modes - %s\n' % (eos_file, modes))
    sys.stdout.flush()

def analyzeBatch(files, modes=allmodes, suffix='', dbfile=None, queue='1nh', submit=True, nevents=999999999):
    #cmd = 'env'
    #output = proc_cmd(cmd)
    #sys.stdout.write('env %s\n' % output)
    #sys.stdout.flush()

    if not os.path.exists(submit_dir):
        cmd = 'mkdir -p %s' % submit_dir
        proc_cmd(cmd)

    analyzer = os.path.realpath(__file__).rstrip('c') #cheap way to remove the c in pyc if it's there

    for filename in files:
        sys.stdout.write('%s\n' % filename)
        sys.stdout.flush()
        run, board = parse_datfilename(filename)

        #Create Submission file
        script_name = os.path.join(submit_dir, 'submit-Run'+run+'_'+board+suffix+'.sh')
        submit_file = open(script_name,'w')
        submit_file.write('#!/bin/bash\n')
        submit_file.write('%s' % analyzer)
        for mode in modes:
            submit_file.write(' -m\'%s\'' % mode)
        if dbfile is not None:
            submit_file.write(' -d\'%s\'' % dbfile)
        submit_file.write(' -n%d' % nevents)
        submit_file.write(' %s\n' % filename)
        submit_file.close()
        cmd = 'chmod a+x %s' % script_name
        proc_cmd(cmd)

        #Submit job
        if submit:
            sys.stdout.write('Submitting %s to %s queue\n' % (script_name, queue))
            cmd = 'bsub -q %s %s' % (queue, script_name)
            proc_cmd(cmd, procdir=submit_dir)

def modified_copy(config_file_path, dest_dir, board, nevents, mount_point=eos_mount_point):
    fname = config_file_path.rpartition('/')[2]
    fpath = os.path.join(dest_dir,fname)

    sys.stdout.write('\tCopying config file from %s to %s\n' % (config_file_path, fpath))

    original = open(config_file_path,'r')
    modified = open(fpath, 'w')

    gearfile = 'gear_cmspixel_telescope_FNAL2013_straight.xml'
    if board == 'PixelTestBoard2':
        gearfile = 'gear_cmspixel_telescope_FNAL2013_tilted.xml'

    for line in original:
        if line.startswith('OutputPath'):
            modified.write('OutputPath = %s\n' % os.path.join(mount_point, processed_dir, board))
        elif line.startswith('NumEvents'):
            modified.write('NumEvents = %d\n' % nevents)
        elif line.startswith('GearFile'):
            modified.write('GearFile = %s\n' % gearfile)
        else:
            modified.write(line)

    modified.close()
    original.close()

    return fpath


if __name__ == '__main__':
    main()
