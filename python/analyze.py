#!/usr/bin/env python
"""
Analyze a run (do any or all of convert, clustering, hitmaker, align, tracks)
   and send the output to a central location
Primarily intended to be submitted to batch queues, but can be run locally

"""

__author__ = "Ulysses Grundler <grundler@cern.ch>"

import sys
import os
from optparse import OptionParser
from helpers import *

#define full set of modes
allmodes =['convert', 'clustering', 'hitmaker', 'align', 'tracks']

job_config_file = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/jobsub/fnal201403/batch.cfg'

work_dir = '/tmp/tracktb/batchsub'
eos_mount_point = '/tmp/tracktb'

def main():
    parser = OptionParser(usage="usage: %prog [options] filename")
    parser.add_option("-m", "--modes",
                        action="append",
                        dest="modes",
                        help="Set modes to run")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("wrong number of arguments")

    if options.modes is None:
        modes = allmodes
    else:
        modes = options.modes

    if len(args) == 1:
        analyze(args[0], modes)
    else:
        analyzeBatch(args, modes)

# eos_file should be full path of file in eos starting with 'eos/' not '/eos/'
def analyze(eos_file, modes=allmodes, run=0, board='unknown'):

    sys.stdout.write('Start analysis of %s:\n\tmodes - %s\n' % (eos_file, modes))
    sys.stdout.flush()

	#source environment for running
    procenv = source_bash(env_file)

    #Find run and board information
    if run == 0 or board == 'unknown':
        r, b = parse_filename(eos_file)
    if run == 0:
        run = r
    if board == 'unknown':
        board = b
    sys.stdout.write('\trun: %s, board: %s\n' % (run, board))
    sys.stdout.flush()

	#Mount EOS and link data from EOS
    rundir = os.path.join(work_dir,'data','cmspixel',str(run).zfill(6))
    cmd = 'mkdir -p %s' % rundir
    proc_cmd(cmd)
    # if not os.path.exists(eos_mount_point+"/eos"):
    #     cmd = 'mkdir -p %s/eos' % eos_mount_point
    #     proc_cmd(cmd)
    # mount_eos(eos_mount_point)
    mount_eos(eos_mount_point)
    cmd = 'ln -sf %s mtb.bin' % os.path.join(eos_mount_point,eos_file)
    proc_cmd(cmd,procdir=rundir)

	#actually run
    for mode in modes:
        sys.stdout.write('\nProcessing: jobsub -c %s %s %s' % (job_config_file, mode, run))
        sys.stdout.flush()

        cmd = 'jobsub -c %s %s %s' % (job_config_file, mode, run)
        output = proc_cmd(cmd, procdir=work_dir, env=procenv)

        sys.stdout.write('OK\n')

	#clean up
    sys.stdout.write('Done processing. Time to clean up\n')
    sys.stdout.flush()
    umount_eos(eos_mount_point)
    # cmd = 'rmdir %s/eos' % eos_mount_point
    # proc_cmd(cmd)
    # cmd = 'cd %s/..; rm -r %s' % (work_dir,work_dir)
    cmd = 'rm -r %s' % work_dir
    proc_cmd(cmd)

    sys.stdout.write('End analysis of %s: modes - %s\n' % (eos_file, modes))
    sys.stdout.flush()

def analyzeBatch(files, modes=allmodes, queue='1nh'):
    submit_dir = '/tmp/tracktb'
    if not os.path.exists(submit_dir):
        cmd = 'mkdir -p %s' % submit_dir
        proc_cmd(cmd)

    analyzer = os.path.realpath(__file__)

    for file in files:
        run, board = parse_filename(file)

        #Create Submission file
        script_name = os.path.join(submit_dir, 'submit-Run'+run+'_'+board+'.sh')
        submit_file = open(script_name,'w')
        submit_file.write('#!/bin/bash\n')
        submit_file.write('%s' % analyzer)
        for mode in modes:
            submit_file.write(' -m\'%s\'' % mode)
        submit_file.write(' %s\n' % file)
        submit_file.close()
        cmd = 'chmod a+x %s' % script_name
        proc_cmd(cmd)

        #Submit job
        sys.stdout.write('Submitting %s to %s queue\n' % (script_name, queue))
        cmd = 'bsub -q %s %s' % (queue, script_name)
        proc_cmd(cmd)

if __name__ == '__main__':
    main()
