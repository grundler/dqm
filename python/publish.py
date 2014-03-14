#!/usr/bin/env python

import sys
import os
from optparse import OptionParser

from config import *
import utils

really_mount = False #to avoid mounting/unmounting if called from dqm

def main():
    parser = OptionParser(usage="usage: %prog -r <run> -b <board>")
    parser.add_option("-r", "--run",
                        action="store",
                        dest="run",
                        help="Set run to publish")
    parser.add_option("-b", "--board",
                        action="store",
                        dest="board",
                        help="Set board for run")
    parser.add_option("-w", "--working_dir",
                        action="store",
                        dest="working_dir",
                        default=default_publish_dir,
                        help="Working directory for job")
    parser.add_option("-d", "--add_to_db",
                        action="store",
                        dest="add_to_db",
                        default=None,
                        help="Insert markers into DB")
    parser.add_option("-e", "--eos_mounted",
                        action="store_true",
                        dest="eos_mounted",
                        default=False,
                        help="run on mounted EOS directory")
    (options, args) = parser.parse_args()

    if not options.run:
        parser.error('No run number given')
    if not options.board:
        parser.error('No board name given')

    publish(options.run, options.board, 
            options.working_dir, options.eos_mounted, options.add_to_db)

def publish(run, board, 
            workingdir=default_publish_dir, eos_mounted=False, add_to_db=None):

    sys.stdout.write('[pub_dqm] run %s ... ' % run)
    sys.stdout.flush()

    if add_to_db is not None:
        open(add_to_db,'a').close()

    procenv = utils.source_bash(dqm_env_file)
    histdir = os.path.join(workingdir, 'histograms')
    if eos_mounted:
        histdir = os.path.join(eos_mount_point, processed_dir, board,'histograms')
        if really_mount:
            utils.mount_eos(eos_mount_point)
    else:
        copy_from_eos(workingdir, processed_dir, run, board)

    cmd = 'dqm %s %s' %(board, str(run).zfill(6))
    output = utils.proc_cmd(cmd, procdir=histdir, env=procenv)
    if debug:
        print output
    sys.stdout.write(' OK.\n')

    if eos_mounted:
        if really_mount:
            utils.umount_eos(eos_mount_point)
    else:
        clean_working_directory(workingdir, run)

def copy_from_eos(workdir, eos_out, run, board):
    if debug:
        sys.stdout.write('Copying necessary root files for run %s\n' % str(run))
        sys.stdout.flush()
    rootfilenames = ['clustering', 'convert', 'hitmaker', 'tracks_prealign']
    lciofilenames = ['decoding.txt']

    subdir = os.path.join(workdir,'lcio')
    if not os.path.exists(subdir):
        os.makedirs(subdir)
    subdir = os.path.join(workdir,'histograms')
    if not os.path.exists(subdir):
        os.makedirs(subdir)

    for name in rootfilenames:
        filename = str(run).zfill(6) + '-' + name + '.root'
        from_file = os.path.join(eos_out, board, 'histograms', filename)
        to_dir = os.path.join(workdir,'histograms')
        cmd = 'xrdcp -f root://eoscms//%s %s/' % (from_file, to_dir)
        utils.proc_cmd(cmd)

    for name in lciofilenames:
        filename = str(run).zfill(6) + '-' + name
        from_file = os.path.join(eos_out, board, 'lcio', filename)
        to_dir = os.path.join(workdir,'lcio')
        cmd = 'xrdcp -f root://eoscms//%s %s/' % (from_file, to_dir)
        utils.proc_cmd(cmd)

def clean_working_directory(myDir, run):
    if debug:
        sys.stdout.write('Cleaning working directory at %s of run %s files\n' % (myDir, str(run)))
        sys.stdout.flush()
    if debug:
        sys.stdout.write('Cleaning %s of files for run %s\n' % (myDir, str(run)))
        sys.stdout.flush()

    subdirs = ['histograms', 'lcio']
    for subdir in subdirs:
        fullpath_to_dir = os.path.join(myDir, subdir)
        cmd = 'ls -1 %s' % fullpath_to_dir
        output, rc = utils.proc_cmd(cmd, get_returncode=True)
        if debug:
            sys.stdout.write('rc: %d\toutput: %s\n' % (rc, output))
            sys.stdout.flush()
        for line in output.split():
            if str(run).zfill(6) in line:
                try:
                    if debug:
                        sys.stdout.write('removing %s\n' % line)
                        sys.stdout.flush()
                    os.remove(os.path.join(fullpath_to_dir,line))
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise


if __name__ == '__main__':
    really_mount = True
    main()
