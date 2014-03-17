#!/usr/bin/env python
"""
Process a run (do any or all of convert, clustering, hitmaker, align, tracks)
   and send the output to a central location
Primarily intended to be submitted to batch queues, but can be run locally

"""

__author__ = "Ulysses Grundler <grundler@cern.ch>"

import sys
import os, errno
import shutil
#from datetime import datetime
from optparse import OptionParser

import logging
log = logging.getLogger(__name__)

from config import *
import utils

really_mount = False #don't mount if called from another script

#define full set of modes
allmodes =['convert', 'clustering', 'hitmaker', 'align', 'tracks']


#working directories
# work_dir = '/tmp/tracktb/batchsub'
# eos_mount_point = '/tmp/tracktb'
# submit_dir = '/afs/cern.ch/cms/Tracker/Pixel/HRbeamtest/batch'

def main():
    parser = OptionParser(usage="usage: %prog [options] runnumber")
    parser.add_option("-m", "--modes",
                        action="store",
                        dest="modes",
                        default="convert,clustering,hitmaker",
                        help="Set modes to run. Comma separated list")
    parser.add_option("-n", "--nevents",
                        action="store",
                        dest="nevents",
                        default=999999999,
                        help="Number of events to process")
    parser.add_option("-w", "--working_dir",
                        action="store",
                        dest="working_dir",
                        default=default_work_dir,
                        help="Working directory for job")
    parser.add_option("-c", "--cfg_file",
                        action="store",
                        dest="cfg_file",
                        default=job_config_file,
                        help="Base configuration file for running")
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
    parser.add_option("-b", "--batch",
                        action="store_true",
                        dest="batch",
                        default=False,
                        help="Set to run on batch queue")
    parser.add_option("-q", "--queue",
                        action="store",
                        dest="queue",
                        default='1nh',
                        help="Set batch queue to submit to (only for remote)")
    parser.add_option("-s", "--suffix",
                        action="store",
                        dest="suffix",
                        default='',
                        help="Suffix for batch job name")
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("Too few arguments")

    modes = options.modes.split(',')

    if not options.batch:
        process_dat(args[0], modes, 
                    options.working_dir, options.cfg_file, options.nevents, 
                    options.eos_mounted, options.add_to_db)
    else:
        process_batch(args[0], modes,
                    options.working_dir, options.cfg_file, options.nevents, 
                    options.eos_mounted, options.add_to_db,
                    options.queue, options.suffix)

####

def process_run(run, modes, 
                workingdir=default_work_dir, cfgfile=job_config_file, nevents=999999999, 
                eos_mounted=False, add_to_db=None):
    #create working directory
    create_working_directory(workingdir, run)

    #get dat file
    datfiles = utils.get_datfile_names(run)
    for dat in datfiles:
        process_dat(dat, modes, 
                workingdir, cfgfile, nevents, 
                eos_mounted, add_to_db)


def process_dat(datfile, modes, 
                workingdir=default_work_dir, cfgfile=job_config_file, nevents=999999999, 
                eos_mounted=False, add_to_db=None):

    run, board = utils.parse_datfilename(datfile)

    #create working directory
    workdir = create_working_directory(workingdir, run, board)

    #get dat file
    if eos_mounted:
        if really_mount:
            utils.mount_eos(eos_mount_point)

    #link dat file and get any slcio files we need
    rundir = os.path.join(workdir,'data','cmspixel',str(run).zfill(6))
    link_from_dir = rundir
    if eos_mounted:
        link_from_dir = os.path.join(eos_mount_point, eosdir, run)
    else:
        utils.cp_dat(os.path.join(eosdir,str(run),datfile), rundir)
        copy_from_eos(workdir, processed_dir, run, board)
    create_data_link(link_from_dir, datfile, workdir, run)

    #set configuration
    outpath = workdir
    if eos_mounted:
        outpath = os.path.join(eos_mount_point, processed_dir, board)
    config_file = get_config(cfgfile, workdir, board, nevents, outpath)

    #submit
    submit(workdir, modes, run, config_file)

    #copy to eos
    if not eos_mounted:
        copy_to_eos(workdir, processed_dir, run, board)
        clean_working_directory(workingdir, run, board)

    if eos_mounted:
        if really_mount:
            utils.umount_eos(eos_mount_point)

    if add_to_db is not None:
        open(add_to_db,'a').close()


def create_working_directory(myDir, run, board=None):
    log.debug('Creating working directory at %s for run %s', myDir, str(run))
    # if debug:
    #     sys.stdout.write('Creating working directory at %s for run %s\n' % (myDir, str(run)))
    #     sys.stdout.flush()
    basedir = myDir
    if board is not None:
        basedir = os.path.join(myDir,board)

    rundir = os.path.join(basedir,'data','cmspixel',str(run).zfill(6))
    if not os.path.exists(rundir):
        os.makedirs(rundir)

    outputdirs = [ 'databases', 'histograms', 'lcio', 'logs']
    for outdir in outputdirs:
        fullpath_to_dir = os.path.join(basedir,outdir)
        if not os.path.exists(fullpath_to_dir):
            os.makedirs(fullpath_to_dir)

    return basedir

def clean_working_directory(myDir, run, board=None):
    log.debug('Cleaning %s of files for run %s', myDir, str(run))
    # if debug:
    #     sys.stdout.write('Cleaning %s of files for run %s\n' % (myDir, str(run)))
    #     sys.stdout.flush()

    basedir = myDir
    if board is not None:
        basedir = os.path.join(myDir,board)

    datapath = os.path.join(basedir,'data','cmspixel',str(run).zfill(6))
    shutil.rmtree(datapath)

    subdirs = ['databases', 'histograms', 'lcio', 'logs']
    for subdir in subdirs:
        fullpath_to_dir = os.path.join(basedir, subdir)
        cmd = 'ls -1 %s' % fullpath_to_dir
        output, rc = utils.proc_cmd(cmd, get_returncode=True)
        for line in output.split():
            if str(run).zfill(6) in line:
                try:
                    os.remove(os.path.join(fullpath_to_dir,line))
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise

def create_data_link(link_from_dir, filename, workdir, run):
    log.debug('Linking dat file for run %s', str(run))
    # if debug:
    #     sys.stdout.write('Linking dat file for run %s\n' % str(run))
    #     sys.stdout.flush()

    rundir = os.path.join(workdir,'data','cmspixel',str(run).zfill(6))
    cmd = 'ln -sf %s mtb.bin' % os.path.join(link_from_dir,filename)
    output = utils.proc_cmd(cmd,procdir=rundir)
    if not os.path.exists(os.readlink(os.path.join(rundir,'mtb.bin'))):
        log.error('mtb.bin is broken symlink')
        # sys.stdout.write('mtb.bin is broken symlink\n')
    
def submit(workdir, modes, run, cfg_file):
    log.info('Start analysis of run %s with modes - %s', run, modes)
    log.info('\tWork from: %s', workdir)
    # sys.stdout.write('Start analysis of run %s with modes - %s\n' % (run, modes))
    # sys.stdout.write('\tWork from: %s\n' % workdir)
    # sys.stdout.flush()

	#source environment for running
    procenv = utils.source_bash(analysis_env_file)

	#actually run
    for mode in modes:
        log.info('Processing: jobsub -c %s %s %s', cfg_file, mode, run)
        # sys.stdout.write('\nProcessing: jobsub -c %s %s %s\n' % (cfg_file, mode, run))
        # sys.stdout.flush()

        cmd = 'jobsub -c %s %s %s' % (cfg_file, mode, run)
        output = utils.proc_cmd(cmd, procdir=workdir, env=procenv)

    log.info('End analysis of %s: modes - %s', run, modes)
    # sys.stdout.write('End analysis of %s: modes - %s\n' % (run, modes))
    # sys.stdout.flush()

def get_config(config_file_path, dest_dir, board, nevents, outpath):
    # if debug:
    #     sys.stdout.write('Getting configuration file\n')
    #     sys.stdout.flush()

    fname = config_file_path.rpartition('/')[2]
    fpath = os.path.join(dest_dir,fname)

    log.debug('Copying config file from %s to %s', config_file_path, fpath)
    #sys.stdout.write('\tCopying config file from %s to %s\n' % (config_file_path, fpath))

    original = open(config_file_path,'r')
    modified = open(fpath, 'w')

    gearfile = 'gear_cmspixel_telescope_FNAL2013_straight.xml'
    if board == 'PixelTestBoard2':
        gearfile = 'gear_cmspixel_telescope_FNAL2013_tilted.xml'

    for line in original:
        if line.startswith('OutputPath'):
            modified.write('OutputPath = %s\n' % outpath)
        elif line.startswith('NumEvents'):
            modified.write('NumEvents = %s\n' % nevents)
        elif line.startswith('GearFile'):
            modified.write('GearFile = %s\n' % gearfile)
        else:
            modified.write(line)

    modified.close()
    original.close()

    return fpath

def copy_to_eos(workdir, eos_out, run, board):
    log.debug('Copying output files for run %s to eos', str(run))
    # if debug:
    #     sys.stdout.write('Copying output files for run %s to eos\n' % str(run))
    #     sys.stdout.flush()

    outputdirs = [ 'databases', 'histograms', 'lcio', 'logs']

    for outdir in outputdirs:
        from_dir = os.path.join(workdir,outdir)
        to_dir = os.path.join(eos_out,board,outdir)
        cmd = 'ls -1 %s' % from_dir
        output = utils.proc_cmd(cmd)
        for line in output.split():
            if line.startswith(str(run).zfill(6)):
                cmd = 'xrdcp -f %s/%s root://eoscms//%s/%s' % (from_dir, line, to_dir, line)
                utils.proc_cmd(cmd)

#copy any slcio files we might need
def copy_from_eos(workdir, eos_out, run, board):
    log.debug('Copying slcio files for run %s from eos', str(run))
    # if debug:
    #     sys.stdout.write('Copying slcio files for run %s from eos\n' % str(run))
    #     sys.stdout.flush()

    slcio_databases = ['prealignment', 'reference']
    slcio_lcio = ['convert', 'clustering', 'hitmaker']

    for slcio in slcio_databases:
        slcioname = str(run).zfill(6) + '-' + slcio + '.slcio'
        from_file = os.path.join(eos_out, board, 'databases', slcioname)
        to_dir = os.path.join(workdir,'databases')
        cmd = 'xrdcp -f root://eoscms//%s %s/' % (from_file, to_dir)
        utils.proc_cmd(cmd)

    for slcio in slcio_lcio:
        slcioname = str(run).zfill(6) + '-' + slcio + '.slcio'
        from_file = os.path.join(eos_out, board, 'lcio', slcioname)
        to_dir = os.path.join(workdir,'lcio')
        cmd = 'xrdcp -f root://eoscms//%s %s/' % (from_file, to_dir)
        utils.proc_cmd(cmd)

def process_batch(datfile, modes, 
                    workingdir=default_work_dir, cfgfile=job_config_file, nevents=999999999, 
                    eos_mounted=False, add_to_db=None,
                    queue='1nh', suffix='', script_dir=None):
    script_name = create_script(datfile, modes,
                                workingdir, cfgfile, nevents,
                                eos_mounted, add_to_db,
                                suffix, script_dir)

    run, board = utils.parse_datfilename(datfile)

    #Submit job
    job_name = 'r'+str(run)+'b'+board[-1]+suffix[:2]
    log.info('Submitting %s to %s queue', script_name, queue)
    #sys.stdout.write('Submitting %s to %s queue\n' % (script_name, queue))
    cmd = 'bsub -q %s -J %s -o %s.out %s' % (queue, job_name, job_name, script_name)
    utils.proc_cmd(cmd, procdir=script_dir)

def create_script(datfile, modes, 
                    workingdir=default_work_dir, cfgfile=job_config_file, nevents=999999999, 
                    eos_mounted=False, add_to_db=None,
                    suffix='', script_dir=None):
    if script_dir is not None:
        if not os.path.exists(script_dir):
            cmd = 'mkdir -p %s' % submit_dir
            utils.proc_cmd(cmd)
    else:
        script_dir = '.'

    analyzer = os.path.realpath(__file__).rstrip('c') #cheap way to remove the c in pyc if it's there

    run, board = utils.parse_datfilename(datfile)

    #Create Submission file
    script_name = os.path.join(script_dir, 'submit-Run'+str(run)+'-'+board+suffix+'.sh')
    submit_file = open(script_name,'w')
    submit_file.write('#!/bin/bash\n')
    submit_file.write('%s' % analyzer)
    submit_file.write(' -m \'%s\'' % ','.join(modes))
    submit_file.write(' -n %d' % nevents)
    submit_file.write(' -w \'%s\'' % workingdir)
    submit_file.write(' -c \'%s\'' % cfgfile)
    if eos_mounted:
        submit_file.write(' -e')
    if add_to_db is not None:
        submit_file.write(' -d \'%s\'' % add_to_db)
    submit_file.write(' %s\n' % datfile)
    submit_file.close()
    cmd = 'chmod a+x %s' % script_name
    utils.proc_cmd(cmd)

    return script_name


if __name__ == '__main__':
    really_mount = True
    main()
