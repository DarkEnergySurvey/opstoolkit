#! /usr/bin/env python

import argparse
import os
import sys
import subprocess
import time
import datetime
import shlex
import argparse

from despydb import DesDbi

""" Stop current cron if still submitting previous jobs from previous cron"""
import commands
def stop_if_already_running():
    """ Exits program if program is already running """

    script_name = os.path.basename(__file__)
    l = commands.getstatusoutput("ps aux | grep -e '%s' | grep -v grep | grep -v vim | awk '{print $2}'| awk '{print $2}' " % script_name)
    if l[1]:
        print "Already running.  Aborting"
        print l[1]
        sys.exit(0)
stop_if_already_running()

"""Create command line arguments"""
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--nite','-n',required=True,help = "i.e., 20140929")
parser.add_argument('--reqnum','-r',required=True,help = "i.e., 1065")
parser.add_argument('--precalnite','-pn',required=True,help = "i.e.,20140928")
parser.add_argument('--precalrun','-pr',required=True,help = "i.e., r1065p01")
parser.add_argument('--section','-s',required=True,help = "i.e., db-desoper")
args=vars(parser.parse_args())

"""Connect to database using user's .desservices file"""
try:
    desdmfile = os.environ["des_services"]
except KeyError:
    desdmfile = None

dbh = DesDbi(desdmfile,args['section'])
cur = dbh.cursor()

""" Get exposure numbers and band for incoming exposures"""
get_expnum_and_band = "select distinct expnum, band from exposure where nite = '%s' and object != 'zeropoint' and object not like '%%pointing%%' and object not like '%%focus%%' and object not like '%%donut%%' and object not like '%%test%%' and object not like '%%junk%%' and obstype='object'" % args['nite']
cur.execute(get_expnum_and_band)
allexpnumnband = cur.fetchall()

""" If no exposures are found. Do nothing."""
if len(allexpnumnband) == 0:
    message = "No exposures found on %s." % datetime.datetime.now()
    print message
    sys.exit(0)

""" Create log file if exposures found"""
log = '%s_firstcut_submit_%s.log' % (args['nite'],time.strftime("%X"))
logfile = open(log,'a')

""" Keep only exposures that have not already been processed"""
for enb in allexpnumnband:
    args['expnum'] = enb[0]
    args['band'] = enb[1]
    was_submitted = "select count(*) from pfw_attempt where unitname= 'D00%s' and reqnum = '%s'" % (args['expnum'],args['reqnum'])
    cur.execute(was_submitted)
    yesorno = cur.fetchone()[0]
    count_unsubmitted = 0
    if yesorno != 0:
        continue
    else:
        count_unsubmitted += 1
        submit = 'firstcut_%s_%s_%s_submit.des' % (args['nite'],args['expnum'],args['band'])
        if os.path.isfile(submit):
            continue
        else:
            """Add template into newly created submit file"""
            submitfile = open(submit,'w')
            template = open('firstcut_submit.des','r').read()
            submitfile.write(template)
            submitfile.close()

        """Add command-line values to end of array"""
        extraincludes = []
        for a in args:
            if a=='section':
                continue
            else:
                line = "%s = %s" %(a,args[a])
                extraincludes.append(line)

        """Determine which block to run: fringe bands vs. non-fringe bands"""
        nofringebands = ['u','g','r','i','VR']
        fringebands = ['z','Y']
        if args['band'] in fringebands:
            label = 'label = %s-fringe' % args['nite']
            block = "blocklist = se_fringe"
            extraincludes.append(label)
            extraincludes.append(block)
            fa = open(submit,'a')
            for item in extraincludes:
                line = "%s\n" % item
                fa.write(line) 
            fa.close()
        elif args['band'] in nofringebands:
            label = 'label = %s-nofringe' % args['nite']
            block = "blocklist = se_nofringe"
            extraincludes.append(label)
            extraincludes.append(block)
            fa = open(submit,'a')
            for item in extraincludes:
                line = "%s\n" % item
                fa.write(line)
            fa.close()
        
        """ Executing dessubmit command""" 
        submit_string = "dessubmit %s" % submit
        submit_command = shlex.split(submit_string)
        command = subprocess.Popen(submit_command,stdout = logfile, stderr = logfile, shell = False)
        time.sleep(30)
    
    """If no unsubmitted exposures found. Print message and quit."""
    if count_unsubmitted == 0:
        message = "No new exposures found on %s" % datetime.datetime.now()
        print message
        sys.exit(0) 

