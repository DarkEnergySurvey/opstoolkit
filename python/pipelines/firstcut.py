#! /usr/bin/env python

from os import path,environ
from sys import exit
from subprocess import Popen
from time import sleep,strftime
from datetime import datetime
from shlex import split
from argparse import ArgumentParser
import time

from opstoolkit import jiracmd,common
from pipelines.firstcut_lib import Get_Firstcut_Inputs,make_comment,append_to_submitfile

class Firstcut():
    def __init__(self,section,**kwargs):
        self.section = section
        self.inputs = Get_Firstcut_Inputs(self.section)
        for key,value in kwargs.items():
            setattr(self,key,value)
    
    def __kwargs__(self):
        kwargs_list = ['precal_tag','parent_key','jira_section','assignee','precalnite','precalrun','campaign','queue_size']
        return sorted(kwargs_list)
    
    def run(self):    
        args = {}
        nite = self.inputs.get_max()[1]
        if self.precalnite:
            precalnite = self.precalnite
            precalrun = self.precalrun
        else:
            precal = self.inputs.find_precal(nite,threshold=7,override=True,tag=self.precal_tag)
            precalnite,precalrun = precal[0],precal[1]
        args['nite'] = nite
        args['precalnite'] = precalnite
        args['precalrun'] = precalrun 

        # Create log file if exposures found.
        log = '%s_firstcut_submit.log' % (nite)
        logfile = open(log,'a')
        wakingup = "%s: Waking up. Checking if I can submit..." % datetime.now()
        logfile.write(wakingup)
       
        allexpnumnband = self.inputs.get_expnums(nite)
        # If no exposures are found, do nothing.
        if len(allexpnumnband) == 0:
            logfile.write("\nNo exposures found. Exiting...")
            exit(0)

        logfile.write("\nChecking for existing jira ticket...")
        #Check to see if Jira ticket exists, if not make one.
        Jira = jiracmd.Jira(self.jira_section)
        issues,count = Jira.search_for_issue(self.parent_key,nite)
        if count == 0:
            logfile.write("\nJIRA ticket does not exist. Creating one...") 
            # Create JIRA ticket...
            description = """
            Precalnite: %s
            Precalrun: %s
            """ % (precalnite,precalrun)
            subticket = str(Jira.create_jira_subtask(self.parent_key,nite,description,self.assignee))
            reqnum = subticket.split('-')[1]
            key = 'DESOPS-%s' % reqnum
        else:
            logfile.write("\nJIRA ticket exists. Will use...")
            reqnum = str(issues[0].key).split('-')[1]
            key = 'DESOPS-%s' % reqnum
        args['reqnum'] = reqnum

        expnums_found = "\n%s exposures found. Seeing what I can submit...\n" % (len(allexpnumnband))
        logfile.write(expnums_found)
        # Keep only exposures that have not already been processed.
        for enb in sorted(allexpnumnband):
            args['expnum'] = enb[0]
            args['band'] = enb[1]
            yesorno = self.inputs.check_submitted(args['expnum'],reqnum) 
            count_unsubmitted = 0
            if yesorno != 0:
                continue
            else:
                # If under queue_size keep submitting jobs...
                if self.queue_size:
                    not_queued = common.less_than_queue('firstcut',self.queue_size)
                else:
                    not_queued = common.less_than_queue('firstcut')
                if not_queued is True:
                    count_unsubmitted += 1
                    submit = 'firstcut_%s_%s_%s_submit.des' % (nite,args['expnum'],args['band'])
                    if path.isfile(submit):
                        continue
                    else:
                        # Add template into newly created submit file.
                        submitfile = open(submit,'w')
                        template = open('firstcut_submit.des','r').read()
                        submitfile.write(template)
                        submitfile.close()
                    
                    # Append WCL arguments to submit WCL...
                    append_to_submitfile(args,submit)

                    # Executing dessubmit command... 
                    submit_string = "dessubmit %s" % submit
                    submit_command = split(submit_string)
                    logfile.flush()
                    command = Popen(submit_command,stdout = logfile, stderr = logfile, shell = False)
                    logfile.write("\nSleeping...\n")
                    sleep(30)
                    # Adding comment to JIRA ticket...
                    comment = make_comment(datetime.now(),nite,reqnum,self.campaign)
                    jira_tix = Jira.get_issue(key) 
                    all_comments = jira_tix.fields.comment.comments
                    if len(all_comments) ==0:
                        Jira.add_jira_comment(key,comment)
                    else:
                        continue
                elif not_queued is False:
                    logfile.write("\nReached queue limit! Exiting...")
                    exit(0)
        
        # If no unsubmitted exposures found. Print message and quit.
        if count_unsubmitted == 0:
            message = "\nNo new exposures found on %s" % datetime.now()
            logfile.write(message)
            print message
            exit(0)
