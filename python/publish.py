#!/usr/bin/env python

import sys
import os
from optparse import OptionParser

from config import *
import utils

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
    (options, args) = parser.parse_args()

    if not options.run:
        parser.error('No run number given')
    if not options.board:
        parser.error('No board name given')

    publish(options.run, options.board)

def publish(run, board):                                                                                
    sys.stdout.write('[pub_dqm] run %s ... ' % run)
    sys.stdout.flush()

    procenv = utils.source_bash(dqm_env_file)
    histdir = os.path.join(eos_mount_point, processed_dir, board, 'histograms')
    utils.mount_eos(eos_mount_point)

    #Indicate we're published in the db
    #db_file_name(fname, job, STATUS.published, insert=True)

    cmd = 'dqm %s %s' %(board, str(run).zfill(6))
    output = utils.proc_cmd(cmd, procdir=histdir, env=procenv)
    if debug: 
        print output
    sys.stdout.write(' OK.\n')

    utils.umount_eos(eos_mount_point)

if __name__ == '__main__':
    main()
