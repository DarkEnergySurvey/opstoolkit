#! /usr/bin/env python

from sys import exit
from argparse import ArgumentParser
from despydb import DesDbi 
from opstoolkit import common

# Stop current cron if still submitting previous jobs from previous cron
common.stop_if_already_running()

# Create command line arguments
parser = ArgumentParser(description=__doc__)
parser.add_argument('--section','-s',required=True,help = "e.g., db-desoper or db-destest")
parser.add_argument('--pipeline','-p',required=True,help = "e.g.,firstcut,supercal,precal,preskytmp,finalcut,coadd")
parser.add_argument('--kwargs','-k',required=False,help = "list of key/value pairs within string,e.g.,kwarg1=value1,kwarg2=value2")
parser.add_argument('--show_kwargs',required=False,action='store_true',help = "Displays available kwargs for pipeline.")
args = parser.parse_args()

# Dynamically load pipeline module
load_pipeline = common.load_pipeline(args.pipeline,args.section)

# If kwargs specified, register them with class
if args.kwargs:
    kwargs_dict = {}
    for item in args.kwargs.split(','):
        k,v = item.split('=')
        kwargs_dict[k]=v

    for key,val in kwargs_dict.iteritems():
        load_pipeline.__dict__[key] = val

# Print available kwargs
if args.show_kwargs:
    print "Available kwargs..."
    print load_pipeline.show_kwargs()        
    exit()

# Execute run function of pipeline class
run_pipeline = load_pipeline.run()
