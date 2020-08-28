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

from opstoolkit import jiracmd_old as jiracmd

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
parser.add_argument('--section','-s',required=True,help = "i.e., db-desoper")
args=vars(parser.parse_args())

"""Connect to database using user's .desservices file"""
try:
    desdmfile = os.environ["des_services"]
except KeyError:
    desdmfile = None

dbh = DesDbi(desdmfile,args['section'])
cur = dbh.cursor()

def check_submitted(date):
    """Check to see if a precal has been submitted with given date"""
    was_submitted = "select count(*) from pfw_attempt where unitname= '%s'" % (date)
    cur.execute(was_submitted)
    count = cur.fetchone()[0]
    return count

def get_max():
    """Get nite of max dflat"""
    max_dflat = "select max(expnum) from exposure where obstype='dome flat'"
    cur.execute(max_dflat)
    max_expnum = cur.fetchone()[0]
    fetch_nite = "select distinct nite from exposure where expnum=%s" % (max_expnum)
    cur.execute(fetch_nite)
    dflat_nite = cur.fetchone()[0]
    return max_expnum,dflat_nite

nite = get_max()[1]
"""Check to see if precal has been submitted with dflat_nite"""
if check_submitted(nite) != 0:
    submitted_string = "%s previously submitted! Exiting..." % (nite)
    print submitted_string
    sys.exit()
else:
    """ Create log file """
    log = '%s_precal_submit_%s.log' % (nite,time.strftime("%X"))
    logfile = open(log,'a')

    """Run check_for_precal_inputs.py"""
    print "Running check_for_precal_inputs.py"
    check_precal_string = "/work/users/mjohns44/desdm/devel/opstoolkit/trunk/bin/check_for_precal_inputs.py --section %s --band u,g,r,i,z,Y,VR --CalReq 20,10,10,10,10,10,10,10 --night %s " % (args['section'],nite)
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
        """Check to see if Jira ticket exists, if not make one"""
        jira = jiracmd.connect_to_jira('jira-desdm')
        exists = jiracmd.search_for_issue('DESOPS-872',nite)
        if exists[1] == 0:
            """ Create JIRA ticket """
            description = 'Input nite: %s' % nite
            subticket = str(jiracmd.create_jira_subtask('DESOPS-872',nite,description,'mjohns44'))
            reqnum = subticket.split('-')[1]
            key = 'DESOPS-%s' % reqnum
        else:
            reqnum = str(exists[0][0].key).split('-')[1]
            key = 'DESOPS-%s' % reqnum
        """Creating submit file"""
        submit = 'precal_%s_submit.des' % (nite)
        if os.path.isfile(submit):
            submitted_string = "%s_r%s previously submitted! Exiting..." %(nite,reqnum)
            print submitted_string
            sys.exit(0)
        else:
            enough_calib_string = "%s Found calibrations. Beginning processing..." % datetime.datetime.now()
            print enough_calib_string
            """Add template into newly created submit file"""
            submitfile = open(submit,'w')
            template = open('precal_submit.des','r').read()
            submitfile.write(template)
            submitfile.close()

            """Add nite,reqnum,label to submitfile"""
            includes = []
            add_nite = "%s = %s" % ('nite',nite)
            includes.append(add_nite)
            reqnumstr = "%s = %s" % ('reqnum',reqnum)
            includes.append(reqnumstr)
            label = 'label = precal_%s' % nite
            includes.append(label)
            
            """Create input expnum lists"""
            listdir = "/home/newframe/mjohns44/Production/Y2N/lists/%s" % nite
            if not os.path.isdir(listdir):
                os.makedirs(listdir)
            bias_file = "%s/%s_bias_exposures.list" % (listdir,nite)
            flat_file = "%s/%s_dflat_exposures.list" % (listdir,nite)
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
            fr = fr.replace('$$$$',nite)
            fw = open(submit,'w')
            fw.write(fr)
            fw.close()
            fa = open(submit,'a')
            for item in includes:
                line = "%s\n" % item
                fa.write(line) 
            fa.close()
            
            """ Executing dessubmit command""" 
            submit_string = "dessubmit %s" % submit
            submit_command = shlex.split(submit_string)
            command = Popen(submit_command,stdout = logfile, stderr = logfile, shell = False)
            comment = """
                Autosubmitted at %s
                -----
                h3. Nite: %s
                -----
                * Reqnum: %s
                * Attnum: 01

                h5. Run Status: 

                h6. Tagged: 
                h6. Datastate:

                h5. Location:  /archive_data/desarchive/OPS/precal/%s-r%s/p01
                """ % (datetime.datetime.now(),nite,reqnum,nite,reqnum)
            jiracmd.add_jira_comment(key,comment)

