#! /usr/bin/env python

import argparse
import os
import sys
from subprocess import Popen,PIPE
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
parser.add_argument('--section','-s',required=True,help = "i.e., db-desoper")
args=vars(parser.parse_args())

"""Connect to database using user's .desservices file"""
try:
    desdmfile = os.environ["des_services"]
except KeyError:
    desdmfile = None

dbh = DesDbi(desdmfile,args['section'])
cur = dbh.cursor()

""" Create log file if exposures found"""
log = '%s_precal_submit_%s.log' % (args['nite'],time.strftime("%X"))
logfile = open(log,'a')

"""Run check_for_precal_inputs.py"""
print "Running check_for_precal_inputs.py"
check_precal_string = "/work/users/mjohns44/desdm/devel/opstoolkit/trunk/bin/check_for_precal_inputs.py --section %s --band u,g,r,i,z,Y,VR --CalReq 20,10,10,10,10,10,10,10 --night %s " % (args['section'],args['nite'])
print check_precal_string
check_precal_command_pieces = shlex.split(check_precal_string) 
check_precal_command = Popen(check_precal_command_pieces,stdout = logfile, stderr = logfile, shell = False)
check_precal_command.communicate()

runornot = 'no'
for line in open(log).readlines():
    if "LETS ROCK!" in line:
        runornot = 'yes'
if runornot == 'no':
    calib_string = "%s Not enough calibrations yet. Exiting..." % datetime.datetime.now()
    print calib_string
    sys.exit(0)
if runornot == 'yes':
    """Check to see if precal has been previously submitted"""
    was_submitted = "select count(*) from pfw_attempt where unitname= '%s' and reqnum = '%s'" % (args['nite'],args['reqnum'])
    cur.execute(was_submitted)
    yesorno = cur.fetchone()[0]
    if yesorno != 0:
        submitted_string = "%s_r%s previously submitted! Exiting..." % (args['nite'],args['reqnum'])
        print submitted_string
        sys.exit(0)
    else:
        """Creating submit file"""
        submit = 'precal_%s_submit.des' % (args['nite'])
        if os.path.isfile(submit):
            submitted_string = "%s_r%s previously submitted! Exiting..." %(args['nite'],args['reqnum'])
            print submitted_string
            sys.exit(0)
        else:
            """Add template into newly created submit file"""
            submitfile = open(submit,'w')
            template = open('precal_submit.des','r').read()
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
            label = 'label = precal_%s' % args['nite']
            extraincludes.append(label)
            
            """Create input expnum lists"""
            listdir = "/home/newframe/mjohns44/Production/Y2N/lists/%s" % args['nite']
            if not os.path.isdir(listdir):
                os.makedirs(listdir)
            bias_file = "%s/%s_bias_exposures.list" % (listdir,args['nite'])
            flat_file = "%s/%s_dflat_exposures.list" % (listdir,args['nite'])
            bias_exp_file = open(bias_file,'w')
            flat_exp_file = open(flat_file,'w')
            for lines in open(log).readlines():
                if 'bias_expnum' in lines:
                    bias_exp_file.write(lines)
                if 'dflat_expnum' in lines:
                    flat_exp_file.write(lines)
            bias_exp_file.close()
            flat_exp_file.close()
    
            """Replace include lines in submit file"""
            fr = open(submit).read()
            fr = fr.replace('$$$$',args['nite'])
            fw = open(submit,'w')
            fw.write(fr)
            fw.close()
            fa = open(submit,'a')
            for item in extraincludes:
                line = "%s\n" % item
                fa.write(line) 
            fa.close()
            
            """ Executing dessubmit command""" 
            submit_string = "dessubmit %s" % submit
            submit_command = shlex.split(submit_string)
            command = Popen(submit_command,stdout = logfile, stderr = logfile, shell = False)
