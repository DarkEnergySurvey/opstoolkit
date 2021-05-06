#! /usr/bin/env python3
from commands import getstatusoutput
from os import path
from sys import exit
import importlib
from subprocess import Popen,PIPE

def less_than_queue(pipeline,queue_size=1000):
    """ Returns True if desstat count is less than specified queue_size"""
    desstat_cmd = Popen(('desstat'),stdout=PIPE)
    grep_cmd = Popen(('grep',pipeline),stdin=desstat_cmd.stdout,stdout=PIPE)
    desstat_cmd.stdout.close()
    count_cmd = Popen(('wc','-l'),stdin=grep_cmd.stdout,stdout=PIPE)
    grep_cmd.stdout.close()
    output,error = count_cmd.communicate()
    if int(output) < int(queue_size):
        return True
    else:
        return False
    
def stop_if_already_running():
    """ Exits program if program is already running """

    script_name = path.basename(__file__)
    l = getstatusoutput("ps aux | grep -e '%s' | grep -v grep | grep -v vim | awk '{print $2}'| awk '{print $2}' " % script_name)
    if l[1]:
        print "Already running.  Aborting"
        print l[1]
        exit(0)

def load_pipeline(pipeline,section):
    try:
        import_str = "pipelines.%s" % pipeline.lower()
        module = importlib.import_module(import_str)
    except:
        print "ImportError! Pipeline '%s' does not exist. Check for $DESDM_SVN/opstoolkit/trunk/python/pipelines/%s.py" % (pipeline.lower(),pipeline.lower())
        exit(1)
    try:
        klass = getattr(module, pipeline.title())(section)
    except:
        print "Class '%s' does not exist!" % pipeline.title()
        exit(1)
    return klass
