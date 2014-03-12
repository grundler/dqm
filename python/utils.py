
import sys
import os
import subprocess
#from datetime import datetime

from config import *

default_mount_point = '/tmp/tracktb'



#
# Functions that are pretty specific to testbeam setup
#

def get_runs(eos_mounted=True):
    runs = []
    cmd = 'ls -1 %s' % daqdir
    if not eos_mounted:
        cmd = '%s ls %s' % (eos, daqdir)
    output = proc_cmd(cmd)
    for line in output.split():
        if len(line) > 6: # skip non-valid run and files. 
            continue

        run = line.zfill(6)
        if not run.isdigit():
            continue

        run = int(run) 

        if run not in runs:
            runs.append(run)

    return runs

def get_board(dat): 
    board = None 
    name = dat.split('_')[0]
    if 'PixelTestBoard' in name: 
        board = name 
    return board 

def parse_datfilename(fname):
    run = 0
    board = 'unknown'
    #Get run and board from filename
    name = fname.rpartition('/')[2] #get part of fname after last '/', this should be the actual file
    splitname = name.split('_spill_')
    if splitname[0].startswith('PixelTestBoard'):
        board = splitname[0]
    run = (splitname[1].split('_'))[0]

    return run, board


def mount_eos(mount_point=default_mount_point):
    # if not os.path.exists(mount_point+"/eos"):
    #     cmd = 'mkdir -p %s/eos' % mount_point
    #     proc_cmd(cmd)

    cmd = '%s -b fuse mount %s/eos' % (eos, mount_point)
    output = proc_cmd(cmd)
    sys.stdout.write('%s' % output)

    global daqdir
    daqdir = os.path.join(mount_point, eosdir)

def umount_eos(mount_point=default_mount_point):
    global daqdir
    daqdir = '/'+eosdir

    if not os.path.exists(mount_point+"/eos"):
        sys.stdout.write('WARNING: Cannot find a point at %s to unmount\n' % mount_point)
        return

    cmd = '%s -b fuse umount %s/eos' % (eos, mount_point)
    output = proc_cmd(cmd)
    #cmd = 'rmdir %s/eos' % mount_point
    #proc_cmd(cmd)

def get_datfile_names(run, eos_mounted=True):
    # sys.stdout.write('getting dat files\n')
    # sys.stdout.flush()

    datfiles = []
    datsize = {}
    maxsize = {'PixelTestBoard1':0, 'PixelTestBoard2':0}
    cmd = 'ls -1 %s/%s' % (daqdir, run)
    if not eos_mounted:
        cmd = '%s ls -1 %s/%s' % (eos, daqdir, run)
    output = proc_cmd(cmd)
    #sys.stdout.write('getting datfiles. output: %s' % output)
    #sys.stdout.flush()
    
    keyword = '.dat'
    for line in output.split():
        if keyword in line:
            #check if transfer marker exists
            t = os.path.join(daqdir, str(run), tprefix+line)
            if eos_mounted:
                if not os.path.exists(t):
                    continue
            else:
                cmd = '%s ls -a %s' % (eos, t)
                stdout, rc = proc_cmd(cmd, get_returncode=True)
                if rc != 0:
                    continue
            #check file before adding i
            f = os.path.join(daqdir, str(run), line)
            filesize = get_filesize(f,eos_mounted) 
            if filesize > 10: 
                # sys.stdout.write('%s : %d\n' % (line, filesize))
                # sys.stdout.flush()

                datsize[line] = filesize
                for s in maxsize:
                    if line.startswith(s):
                        if filesize > maxsize[s]:
                            maxsize[s] = filesize
                #datfiles.append(line)

    for key in datsize:
        for s in maxsize:
            if key.startswith(s):
                if datsize[key] == maxsize[s]:
                    datfiles.append(key)

    # sys.stdout.write('datfiles: %s \n' % datfiles)
    # sys.stdout.flush()


    return datfiles

def cp_dat(dat, copyto_dir):
    if not os.path.exists(copyto_dir):
        os.makedirs(copyto_dir)
        # cmd = "mkdir -p %s" % copyto_dir
        # proc_cmd(cmd)
    # srcfile = os.path.join(daqdir, str(run), dat)
    
    #cmd = '%s cp %s %s' %(eos, dat, copyto_dir)
    cmd = 'xrdcp -f root://eoscms//%s %s/' % (dat, copyto_dir)
    sys.stdout.write('%s\n' % cmd)
    sys.stdout.flush()
    output, rc = proc_cmd(cmd, get_returncode=True)
    sys.stdout.write('%s\n' % output)
    sys.stdout.flush()
    # if debug:
    #     print cmd 
    #     print output 
    return rc


#
# Functions that should be fairly generic
#

def proc_cmd(cmd, test=False, verbose=1, procdir=None, env=os.environ, get_returncode=False):
    if test:
        sys.stdout.write(cmd+'\n')
        return 

    cwd = os.getcwd()
    if procdir != None:
        os.chdir(procdir)

    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, env=env)
    process.wait()
    stdout = process.communicate()[0]
    rc = process.returncode
    if 'error' in stdout:
        sys.stdout.write(stdout)
    if procdir != None:
        os.chdir(cwd)
    if get_returncode:
        return stdout, rc
    return stdout

def get_filesize(f, eos_mounted=True):
    cmd = 'ls -l %s' % f
    if not eos_mounted:
        cmd = '%s ls -l %s' % (eos, f)
    output = proc_cmd(cmd)
    items = output.split()
    size = items[4]
    if not size.isdigit(): 
        sys.stdout.write('WARNING: not able to get file size \n')
        raise NameError(output)
    size = int(size)
    return size 

def source_bash(f):
    pipe = subprocess.Popen(". %s; env" % f, stdout=subprocess.PIPE, shell=True)
    output = pipe.communicate()[0]
    env = {}
    for line in output.splitlines():
        items = line.split("=", 1)
        if len(items) < 2:
            continue

        #this is a kluge to fix a problem I'm seeing
        if items[0] == 'module':
            items[1] += '\n}'

        env[items[0]]= items[1]
    return env
