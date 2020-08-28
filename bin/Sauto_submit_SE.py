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

def get_max():
    """Get nite of max object"""
    max_object = "select max(expnum) from exposure where obstype='object' and propid='2012B-0001'"
    cur.execute(max_object)
    max_expnum = cur.fetchone()[0]
    fetch_nite = "select distinct nite from exposure where expnum=%s" % (max_expnum)
    cur.execute(fetch_nite)
    object_nite = cur.fetchone()[0]
    return max_expnum,object_nite

def find_precal(date):
    oneday,twoday,threeday = datetime.timedelta(days=1),datetime.timedelta(days=2),datetime.timedelta(days=3)
    nitestring = "%s-%s-%s" % (date[:4],date[4:6],date[6:])
    nite = datetime.datetime.strptime(nitestring,"%Y-%m-%d")
    yesterday,twodaysago,threedaysago = str((nite - oneday).date()).replace('-',''),str((nite - twoday).date()).replace('-',''),str((nite - threeday).date()).replace('-','')
    find_precal_1 = "select distinct unitname,reqnum,attnum from pfw_attempt where unitname='%s'" % yesterday
    cur.execute(find_precal_1)
    results_1 = cur.fetchall()
    max_1 = len(results_1) -1
    if len(results_1) != 0:
        precal_unitname,precal_reqnum,precal_attnum = results_1[max_1][0],results_1[max_1][1],results_1[max_1][2]
        status_precal_1 = "select distinct status from task where id in (select distinct task_id from pfw_attempt where unitname='%s' and reqnum=%s and attnum=%s)" % (precal_unitname,precal_reqnum,precal_attnum)
        cur.execute(status_precal_1)
        status_1 = cur.fetchone()[0]
    if len(results_1) == 0 or status_1 == 1 or status_1 is None:
        find_precal_2 = "select distinct unitname,reqnum,attnum from pfw_attempt where unitname='%s'" % twodaysago
        cur.execute(find_precal_2)
        results_2 = cur.fetchall()
        max_2 = len(results_2) - 1
        if len(results_2) != 0:
            precal_unitname,precal_reqnum,precal_attnum = results_2[max_2][0],results_2[max_2][1],results_2[max_2][2]
            status_precal_2 = "select distinct status from task where id in (select distinct task_id from pfw_attempt where unitname='%s' and reqnum=%s and attnum=%s)" % (precal_unitname,precal_reqnum,precal_attnum)
            cur.execute(status_precal_2)
            status_2 = cur.fetchone()[0]    
        if len(results_2) == 0 or status_2 ==1 or status_2 is None:
            find_precal_3 = "select distinct unitname,reqnum,attnum from pfw_attempt where unitname='%s'" % threedaysago
            cur.execute(find_precal_3)
            results_3 = cur.fetchall()    
            max_3 = len(results_3) -1
            if len(results_3) != 0:
                precal_unitname,precal_reqnum,precal_attnum = results_3[max_3][0],results_3[max_3][1],results_3[max_3][2]
                status_precal_3 = "select distinct status from task where id in (select distinct task_id from pfw_attempt where unitname='%s' and reqnum=%s and attnum=%s)" % (precal_unitname,precal_reqnum,precal_attnum)
                cur.execute(status_precal_3)
                status_3 = cur.fetchone()[0]
            if len(results_3) ==0 or status_3 == 1 or status_3 is None:
                max_tagged = "select distinct unitname,reqnum, attnum from ops_proctag where tag = 'Y2N_PRECAL' and unitname in (select max(unitname) from ops_proctag where tag = 'Y2N_PRECAL' and unitname < %s)" % date
                cur.execute(max_tagged) 
                results_4 = cur.fetchall()
                precal_unitname,precal_reqnum,precal_attnum = results_4[0][0],results_4[0][1],results_4[0][2]
    precal_nite = precal_unitname
    precal_run = 'r%sp0%s' % (precal_reqnum,precal_attnum)
    return precal_nite, precal_run   

nite = get_max()[1]
args['nite'] = nite
precal = find_precal(nite)
precalnite,precalrun = precal[0],precal[1]
args['precalnite'] = precalnite
args['precalrun'] = precalrun

print "selecting exposures to submit..."
""" Get exposure numbers and band for incoming exposures"""
get_expnum_and_band = "select distinct expnum, band from exposure where nite = '%s' and object not like '%%pointing%%' and object not like '%%focus%%' and object not like '%%donut%%' and object not like '%%test%%' and object not like '%%junk%%' and obstype='object'" % nite
cur.execute(get_expnum_and_band)
allexpnumnband = cur.fetchall()

""" If no exposures are found. Do nothing."""
if len(allexpnumnband) == 0:
    message = "No exposures found on %s." % datetime.datetime.now()
    print message
    sys.exit(0)

print "checking for existing jira ticket..."
"""Check to see if Jira ticket exists, if not make one"""
jira = jiracmd.connect_to_jira('jira-desdm')
exists = jiracmd.search_for_issue('DESOPS-871',nite)
if exists[1] == 0:
    print "JIRA ticket does not exist. Creating one..."
    """ Create JIRA ticket """
    description = """
    Precalnite: %s
    Precalrun: %s
    """ % (precalnite,precalrun)
    subticket = str(jiracmd.create_jira_subtask('DESOPS-871',nite,description,'mjohns44'))
    reqnum = subticket.split('-')[1]
    key = 'DESOPS-%s' % reqnum
else:
    print "JIRA ticket exists. Will use..."
    reqnum = str(exists[0][0].key).split('-')[1]
    key = 'DESOPS-%s' % reqnum
args['reqnum'] = reqnum

""" Create log file if exposures found"""
log = '%s_firstcut_submit_%s.log' % (nite,time.strftime("%X"))
logfile = open(log,'a')

print "%s exposures found..." % (len(allexpnumnband))
""" Keep only exposures that have not already been processed"""
for enb in sorted(allexpnumnband):
    args['expnum'] = enb[0]
    args['band'] = enb[1]
    was_submitted = "select count(*) from pfw_attempt where unitname= 'D00%s' and reqnum = '%s'" % (args['expnum'],reqnum)
    cur.execute(was_submitted)
    yesorno = cur.fetchone()[0]
    count_unsubmitted = 0
    if yesorno != 0:
        continue
    else:
        count_unsubmitted += 1
        submit = 'firstcut_%s_%s_%s_submit.des' % (nite,args['expnum'],args['band'])
        if os.path.isfile(submit):
            continue
        else:
            """Add template into newly created submit file"""
            submitfile = open(submit,'w')
            template = open('firstcut_submit.des','r').read()
            submitfile.write(template)
            submitfile.close()

        """Add determined nite,precal values to end of array"""
        extraincludes = []
        for a in args:
            if a == 'section':
                continue
            else:
                line = "%s = %s" % (a,args[a])
                extraincludes.append(line)

        """Determine which block to run: fringe bands vs. non-fringe bands"""
        nofringebands = ['u','g','r','i','VR']
        fringebands = ['z','Y']
        if args['band'] in fringebands:
            label = 'label = %s-fringe' % nite
            block = "blocklist = se_fringe"
            extraincludes.append(label)
            extraincludes.append(block)
            fa = open(submit,'a')
            for item in extraincludes:
                line = "%s\n" % item
                fa.write(line) 
            fa.close()
        elif args['band'] in nofringebands:
            label = 'label = %s-nofringe' % nite
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
        comment = """
        Autosubmit started at %s
        -----
        h3. Nite: %s
        -----
        * Reqnum: %s
        * Attnum: 01

        h5. Run Status: 

        h6. Merged: 
        h6. Tagged: 
        h6. Datastate:

        h5. Location:  /archive_data/desarchive/OPS/firstcut/%s-r%s/D00*/p01
        """ % (datetime.datetime.now(),nite,reqnum,nite,reqnum)
        jira_tix = jira.issue(key) 
        all_comments = jira_tix.fields.comment.comments
        if len(all_comments) ==0:
            jiracmd.add_jira_comment(key,comment)
        else:
            continue
    
"""If no unsubmitted exposures found. Print message and quit."""
if count_unsubmitted == 0:
    message = "No new exposures found on %s" % datetime.datetime.now()
    print message
    sys.exit(0) 

