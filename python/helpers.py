
import sys
import os
import subprocess

#eos command
eos="/afs/cern.ch/project/eos/installation/cms/bin/eos.select"

#Global variables specific to testbeam setup
dataset='FNAL2013'
eosdir = 'eos/cms/store/cmst3/group/tracktb/'+dataset
processed_dir = eosdir+ '/processed'

default_mount_point = '/tmp/tracktb'
daqdir=''

#
# Functions that are pretty specific to testbeam setup
#

def get_runs():
    runs = []
    cmd = 'ls -1 %s' % daqdir
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
    if not os.path.exists(mount_point+"/eos"):
        cmd = 'mkdir -p %s/eos' % mount_point
        proc_cmd(cmd)

    cmd = '%s -b fuse mount %s/eos' % (eos, mount_point)
    output = proc_cmd(cmd)
    sys.stdout.write('%s' % output)

    global daqdir
    daqdir = os.path.join(mount_point, eosdir)

def umount_eos(mount_point=default_mount_point):
    global daqdir
    daqdir = ''

    if not os.path.exists(mount_point+"/eos"):
        sys.stdout.write('WARNING: Cannot find a point at %s to unmount\n' % mount_point)
        return

    cmd = '%s -b fuse umount %s/eos' % (eos, mount_point)
    output = proc_cmd(cmd)
    cmd = 'rmdir %s/eos' % mount_point
    proc_cmd(cmd)

def get_datfiles(run): 
    datfiles = []
    cmd = 'ls -1 %s/%s' % (daqdir, run)
    output = proc_cmd(cmd)
    
    keyword = '.dat'
    for line in output.split():
        if keyword in line:
            f = os.path.join(daqdir, str(run), line)
            filesize = get_filesize(f) 
            if filesize > 10: 
                datfiles.append(line)

    return datfiles

# def ln_dat(run, board, dat, force=False):
# 	dstdir = get_rundir(run, board) 
# 	if os.path.exists(dstdir) and not force: 
# 	   sys.stdout.write('Skip linking %s_%s.\n' %( run, board))
# 	   return 

# 	cmd = "mkdir -p %s; cd %s" %(dstdir,dstdir) 
# 	proc_cmd(cmd)

# 	srcfile = os.path.join(datadir, eosdir, str(run), dat)
# 	cmd = 'ln -s %s .; cd -' %(srcfile)
# 	output = proc_cmd(cmd)

#
# Functions that should be fairly generic
#

def proc_cmd(cmd, test=False, verbose=1, procdir=None, env=os.environ):
    if test:
        sys.stdout.write(cmd+'\n')
        return 

    cwd = os.getcwd()
    if procdir != None:
        os.chdir(procdir)

    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, env=env)
    process.wait()
    stdout = process.communicate()[0]
    if 'error' in stdout:
        sys.stdout.write(stdout)
    if procdir != None:
        os.chdir(cwd)
    return stdout

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

def source_bash(f):
    pipe = subprocess.Popen(". %s; env" % f, stdout=subprocess.PIPE, shell=True)
    output = pipe.communicate()[0]
    #env = dict((line.split("=", 1) for line in output.splitlines()))
    env = {}
    for line in output.splitlines():
        items = line.split("=", 1)
        if len(items) < 2:
            continue

        env[items[0]]= items[1]
    return env
