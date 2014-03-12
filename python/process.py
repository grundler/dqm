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

from config import *
import utils

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
                        default='/tmp/tracktb/working',
                        help="Working directory for job")
    parser.add_option("-c", "--cfg_file",
                        action="store",
                        dest="cfg_file",
                        default=job_config_file,
                        help="Base configuration file for running")
    parser.add_option("-e", "--eos_mounted",
                        action="store_true",
                        dest="eos_mounted",
                        default=False,
                        help="run on mounted EOS directory")
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
    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.error("Too few arguments")

    modes = options.modes.split(',')

    process_run(args[0], modes, options.working_dir, options.cfg_file, options.nevents, options.eos_mounted)


def process_run(run, modes, workingdir=default_work_dir, cfgfile=job_config_file, nevents=999999999, eos_mounted=False):
    #create working directory
    create_working_directory(workingdir, run)

    #get dat file
    datfiles = utils.get_datfile_names(run,eos_mounted)
    for dat in datfiles:
        board = utils.get_board(dat)

        #link dat file and get any slcio files we need
        rundir = os.path.join(workingdir,'data','cmspixel',str(run).zfill(6))
        link_from_dir = rundir
        if eos_mounted:
            link_from_dir = os.path.join(eos_mount_point, eosdir, run)
        else:
            utils.cp_dat(os.path.join(eosdir,run,dat), rundir)
            copy_from_eos(workingdir, processed_dir, run, board)
        create_data_link(link_from_dir, dat, workingdir, run)

        #set configuration
        outpath = workingdir
        if eos_mounted:
            outpath = os.path.join(eos_mount_point, processed_dir, board)
        config_file = get_config(cfgfile, workingdir, board, nevents, outpath)

        #submit
        submit(workingdir, modes, run, config_file)

        #copy to eos
        if not eos_mounted:
            copy_to_eos(workingdir, processed_dir, run, board)
            clean_working_directory(workingdir, run)

def create_working_directory(myDir, run):
    rundir = os.path.join(myDir,'data','cmspixel',str(run).zfill(6))
    if not os.path.exists(rundir):
        os.makedirs(rundir)

    outputdirs = [ 'databases', 'histograms', 'lcio', 'logs']
    for outdir in outputdirs:
        fullpath_to_dir = os.path.join(myDir,outdir)
        if not os.path.exists(fullpath_to_dir):
            os.makedirs(fullpath_to_dir)

def clean_working_directory(myDir, run):
    datapath = os.path.join(myDir,'data','cmspixel',str(run).zfill(6))
    shutil.rmtree(datapath)

    subdirs = ['databases', 'histograms', 'lcio', 'logs']
    for subdir in subdirs:
        fullpath_to_dir = os.path.join(myDir, subdir)
        cmd = 'ls -1 %s' % fullpath_to_dir
        output = utils.proc_cmd(cmd)
        for line in output.split():
            if line.startswith(str(run).zfill(6)):
                try:
                    os.remove(line)
                except OSError as e:
                    if e.errno != errno.ENOENT
                        raise

def create_data_link(link_from_dir, filename, workdir, run):
    rundir = os.path.join(workdir,'data','cmspixel',str(run).zfill(6))
    cmd = 'ln -sf %s mtb.bin' % os.path.join(link_from_dir,filename)
    output = utils.proc_cmd(cmd,procdir=rundir)
    if not os.path.exists(os.readlink(os.path.join(rundir,'mtb.bin'))):
        sys.stdout.write('mtb.bin is broken symlink\n')
    
def submit(workdir, modes, run, cfg_file):
    sys.stdout.write('Start analysis of run %s with modes - %s\n' % (run, modes))
    sys.stdout.write('\tWork from: %s\n' % workdir)
    sys.stdout.flush()

	#source environment for running
    procenv = utils.source_bash(analysis_env_file)

	#actually run
    for mode in modes:
        sys.stdout.write('\nProcessing: jobsub -c %s %s %s\n' % (cfg_file, mode, run))
        sys.stdout.flush()

        cmd = 'jobsub -c %s %s %s' % (cfg_file, mode, run)
        output = utils.proc_cmd(cmd, procdir=workdir, env=procenv)

        sys.stdout.write('OK\n')

    sys.stdout.write('End analysis of %s: modes - %s\n' % (run, modes))
    sys.stdout.flush()

def get_config(config_file_path, dest_dir, board, nevents, outpath):
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
            modified.write('OutputPath = %s\n' % outpath)
        elif line.startswith('NumEvents'):
            modified.write('NumEvents = %d\n' % nevents)
        elif line.startswith('GearFile'):
            modified.write('GearFile = %s\n' % gearfile)
        else:
            modified.write(line)

    modified.close()
    original.close()

    return fpath

def copy_to_eos(workdir, eos_out, run, board):
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
    outputdirs = ['databases', 'lcio']
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

    # for outdir in outputdirs:
    #     from_dir = os.path.join(eos_out,board,outdir)
    #     to_dir = os.path.join(workdir,outdir)
    #     cmd = '%s ls -1 %s' % (eos, from_dir)
    #     output = utils.proc_cmd(cmd)
    #     for line in output.split():
    #         if line.startswith(str(run).zfill(6)):
    #             cmd = 'xrdcp -f root://eoscms//%s/%s %s/' % (from_dir, line, to_dir)
    #             utils.proc_cmd(cmd)


def create_script(file, modes, suffix='', dbfile=None, script_dir=None):
    if not os.path.exists(submit_dir):
        cmd = 'mkdir -p %s' % submit_dir
        utils.proc_cmd(cmd)

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
        utils.proc_cmd(cmd)

        #Submit job
        if submit:
            job_name = 'r'+run+'_b'+board[-1]+suffix[:2]
            sys.stdout.write('Submitting %s to %s queue\n' % (script_name, queue))
            cmd = 'bsub -q %s -J %s -o %s.out %s' % (queue, job_name, job_name, script_name)
            utils.proc_cmd(cmd, procdir=submit_dir)

# def analyzeBatch(files, modes=allmodes, suffix='', dbfile=None, queue='1nh', submit=True, nevents=999999999):
#     if not os.path.exists(submit_dir):
#         cmd = 'mkdir -p %s' % submit_dir
#         proc_cmd(cmd)

#     analyzer = os.path.realpath(__file__).rstrip('c') #cheap way to remove the c in pyc if it's there

#     for filename in files:
#         sys.stdout.write('%s\n' % filename)
#         sys.stdout.flush()
#         run, board = parse_datfilename(filename)

#         #Create Submission file
#         script_name = os.path.join(submit_dir, 'submit-Run'+run+'_'+board+suffix+'.sh')
#         submit_file = open(script_name,'w')
#         submit_file.write('#!/bin/bash\n')
#         submit_file.write('%s' % analyzer)
#         for mode in modes:
#             submit_file.write(' -m\'%s\'' % mode)
#         if dbfile is not None:
#             submit_file.write(' -d\'%s\'' % dbfile)
#         submit_file.write(' -n%d' % nevents)
#         submit_file.write(' %s\n' % filename)
#         submit_file.close()
#         cmd = 'chmod a+x %s' % script_name
#         proc_cmd(cmd)

#         #Submit job
#         if submit:
#             job_name = 'r'+run+'_b'+board[-1]+suffix[:2]
#             sys.stdout.write('Submitting %s to %s queue\n' % (script_name, queue))
#             cmd = 'bsub -q %s -J %s -o %s.out %s' % (queue, job_name, job_name, script_name)
#             proc_cmd(cmd, procdir=submit_dir)

# # eos_file should be full path of file in eos starting with 'eos/' not '/eos/'
# def analyze(eos_file, modes, run=0, board='unknown', dbfile=None, nevents=999999999, viacopy=False):
#     sys.stdout.write('Start analysis of %s:\n\tmodes - %s\n' % (eos_file, modes))
#     sys.stdout.flush()

# 	#source environment for running
#     procenv = source_bash(env_file)

#     #Find run and board information
#     if run == 0 or board == 'unknown':
#         r, b = parse_datfilename(eos_file)
#     if run == 0:
#         run = r
#     if board == 'unknown':
#         board = b
#     sys.stdout.write('\trun: %s, board: %s\n' % (run, board))
#     sys.stdout.flush()

#     #create a unique string to keep eos mounts separate so they don't disappear when running multiple jobs
#     mytime = str(datetime.today()).split(' ')[1].replace(':','').replace('.','')
#     mount_point = eos_mount_point+mytime
#     workdir = work_dir+mytime

#     sys.stdout.write('\tMount eos at: %s\n' % mount_point)
#     sys.stdout.write('\tWork from: %s\n' % workdir)

# 	#Create working directory structure
#     rundir = os.path.join(workdir,'data','cmspixel',str(run).zfill(6))
#     cmd = 'mkdir -p %s' % rundir
#     output = proc_cmd(cmd)

#     #copy or link data from EOS
#     if not viacopy:
#         mount_eos(mount_point)
#         cmd = 'ln -sf %s mtb.bin' % os.path.join(mount_point,eos_file)
#     else:
#         cp_dat(eos_file)
#         cmd = 'ln -sf %s mtb.bin' % os.path.join(copyto_dir, eos_file)
#     output = proc_cmd(cmd,procdir=rundir)
#     if not os.path.exists(os.readlink(os.path.join(rundir,'mtb.bin'))):
#         sys.stdout.write('mtb.bin is broken symlink\n')

#     #Create modified copy of config file, appropriate to the board
#     cfg_file = modified_copy(job_config_file, workdir, board, nevents, mount_point=mount_point)

# 	#actually run
#     for mode in modes:
#         sys.stdout.write('\nProcessing: jobsub -c %s %s %s\n' % (cfg_file, mode, run))
#         sys.stdout.flush()

#         cmd = 'jobsub -c %s %s %s' % (cfg_file, mode, run)
#         output = proc_cmd(cmd, procdir=workdir, env=procenv)

#         sys.stdout.write('OK\n')

#     #If requested, create a file indicating this is completed
#     if dbfile is not None:
#         open(dbfile,'a').close()

# 	#clean up
#     sys.stdout.write('Done processing. Time to clean up\n')
#     sys.stdout.flush()
#     umount_eos(mount_point)
#     cmd = 'rm -r %s' % workdir
#     proc_cmd(cmd)

#     sys.stdout.write('End analysis of %s: modes - %s\n' % (eos_file, modes))
#     sys.stdout.flush()


# def modified_copy(config_file_path, dest_dir, board, nevents, mount_point=eos_mount_point):
#     fname = config_file_path.rpartition('/')[2]
#     fpath = os.path.join(dest_dir,fname)

#     sys.stdout.write('\tCopying config file from %s to %s\n' % (config_file_path, fpath))

#     original = open(config_file_path,'r')
#     modified = open(fpath, 'w')

#     gearfile = 'gear_cmspixel_telescope_FNAL2013_straight.xml'
#     if board == 'PixelTestBoard2':
#         gearfile = 'gear_cmspixel_telescope_FNAL2013_tilted.xml'

#     for line in original:
#         if line.startswith('OutputPath'):
#             modified.write('OutputPath = %s\n' % os.path.join(mount_point, processed_dir, board))
#         elif line.startswith('NumEvents'):
#             modified.write('NumEvents = %d\n' % nevents)
#         elif line.startswith('GearFile'):
#             modified.write('GearFile = %s\n' % gearfile)
#         else:
#             modified.write(line)

#     modified.close()
#     original.close()

#     return fpath



if __name__ == '__main__':
    main()
