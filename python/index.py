#!/usr/bin/env python

import sys
import os
import time
import re

import logging
FORMAT = '%(asctime)s: %(name)s - %(levelname)s: %(message)s' 
DATEFMT = '%H:%M:%S'
logging.basicConfig(format=FORMAT, datefmt=DATEFMT, level=logging.INFO)
log = logging.getLogger("index")


import HTML
import utils
from config import *

def main():
    index()

def index():
    log.info('Make index')
    # sys.stdout.write('[make index] ... ')
    # sys.stdout.flush()
    procenv = utils.source_bash(dqm_env_file)
    targetdir = procenv['TARGETDIRECTORY']

    #runs = []
    run_status = {}

    log.debug('Get job statuses')
    for j in range(JOBS.nJobs):
        for s in range(STATUS.nStatus):
            dbsubdir = os.path.join(dbdir,JOBS.prefix[j],STATUS.prefix[s])
            # sys.stdout.write('subdir %s\n' % dbsubdir)
            dblist = os.listdir("%s" % dbsubdir)
            for line in dblist:
                # sys.stdout.write('\t%s\n' % line)
                run, board, job, status = parse_db(line)
                # sys.stdout.write('\t\t%s %s %s %s\n' % (str(run), board, JOBS.prefix[job], STATUS.prefix[status]))
                # sys.stdout.flush()
                # if run not in runs:
                #     runs.append(run)
                label = str(run).zfill(6)+'_'+board
                if (label) not in run_status:
                    run_status[label] = {}
                if job not in run_status[label] or run_status[label][job] < status:
                    run_status[label][job] = status

    # runs = sorted(runs, reverse=True)

    log.debug('Create table')
    header_row = ['Run']
    header_row.extend(JOBS.prefix[:-1])
    header_row.append('Events valid')
    t = HTML.Table(header_row=header_row)
    for run_board in sorted(run_status, reverse=True):
        run_link = HTML.link(run_board, '%s' %run_board)
        row = [run_link]

        r_b = run_board.split('_')
        nevts = get_num_events(r_b[0], r_b[1], targetdir)

        # for job in run_status[run_board]:
        #     color = STATUS.colors[run_status[run_board][job]]
        #     colored_result = HTML.TableCell(STATUS.prefix[run_status[run_board][job]], bgcolor=color)
        #     row.append(colored_result)

        for job in range(JOBS.nJobs):
            if job in run_status[run_board]:
                color = STATUS.colors[run_status[run_board][job]]
                colored_result = HTML.TableCell(STATUS.prefix[run_status[run_board][job]], bgcolor=color)
            else:
                color = STATUS.colors[STATUS.unknown]
                colored_result = HTML.TableCell(STATUS.prefix[STATUS.unknown], bgcolor=color)
            row.append(colored_result)

        evtscell = HTML.TableCell(str(nevts), align='right')
        row.append(evtscell)
        t.rows.append(row)

    htmlcode = str(t)

    html_header = '''<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8" />
    <title>Test Beam DQM - clusters</title>
    <meta name="keywords" content="CERN CMS tracker upgrade" />
    <meta name="description" content="CMS Tracker upgrade summary page" />
    <link href=".style/default.css" rel="stylesheet" type="text/css" />
    <link rel="shortcut icon" type="image/x-icon" href=".style/images/favicon.ico">
 </head>
  <body>
      <div id="header">
    <h1>
    <a href="index.html">%s Test Beam DQM</a>
    </h1>
    </div>
    <div id="content">
    ''' % dataset 

    html_footer = '''<div id="footer">
    <p>Page created on %s </p>
    <p>&copy; <a href="mailto:Xin.Shi@cern.ch"> Xin Shi</a> 2013 </p>
    </div>
    </div>
    </body>
    </html>''' %  time.strftime("%Y-%m-%d %H:%M:%S GMT", time.gmtime())


    index = os.path.join(targetdir, 'index.html')
    fo = open(index, 'w')
    fo.write(html_header)
    fo.write(htmlcode)
    fo.write(html_footer)
    fo.close()
    log.info('Indexing done')
    # sys.stdout.write(' OK.\n')

def parse_db(dbfile):
    status = STATUS.unknown
    job = JOBS.unknown
    board = 'unknown'
    run = 0

    stem_suffix = dbfile.partition('.dat.')

    #get run and board
    run, board = utils.parse_datfilename(stem_suffix[0])

    #Get job and status
    labels = (stem_suffix[2]).partition('.')
    for j in range(JOBS.nJobs):
        if labels[0] == JOBS.prefix[j]:
            job = j

    for s in range(STATUS.nStatus):
        if labels[2] == STATUS.prefix[s]:
            status = s
    
    return run, board, job, status

def get_num_events(run, board, targetdir):
    log.debug('Get valid events for run %s', str(run))
    searchstr = 'Events valid:'
    f = os.path.join(targetdir,str(run)+'_'+board,'decoding.html')
    if os.path.isfile(f):
        with open(f) as decf:
            for line in decf:
                if searchstr in line:
                    log.debug('\t%s', line)
                    return int(re.findall('\d+', line)[0])

    log.debug('Could not find events')
    return -1



if __name__ == '__main__':
    main()
