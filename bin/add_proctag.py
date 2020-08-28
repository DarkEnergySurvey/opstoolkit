#! /usr/bin/env python

""" This code takes an unitname,reqnum,attnum combination and applies a tag to it in the ops_proctag table. This combination is known as a "run." The code can also take a comma-separated list of "runs" per line and adds the "run" to the ops_proctag table, then updates the tag for each "run" specified. Allowable tags are defined in the ops_proctag_def table.

Example list: 
#unitname,reqnum,attnum,tag
D00155739,70,01,Y1A1_FINALCUT
D00155742,70,02,Y1A1_FINALCUT
D00205550,71,01,Y1A1_PRECAL


Created by : Michael D. Johnson, August, 6th 2014
Version 1
"""

""" Importing necessary modules"""
import argparse
import os
import sys
from despydb import DesDbi 

"""Create command line arguments"""
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--file','-f',help="File of line-separated runs to be tagged")
parser.add_argument('--tag','-t',)
parser.add_argument('--section','-s',)
parser.add_argument('--reqnum','-r',)
parser.add_argument('--unitname','-u',)
parser.add_argument('--attnum','-a')
parser.add_argument('--pfw_attempt_id','-i')
parser.add_argument('--update','-d',action='store_true',default=False)
args = parser.parse_args()

file = args.file
section = args.section

run = []
if file is None:
    unitname = args.unitname
    reqnum = args.reqnum
    attnum = args.attnum
    tag = args.tag
    if unitname is None or reqnum is None or attnum is None or tag is None:
        print "Must specify all: unitname,reqnum,attnum,tag!"
        sys.exit(1)
    run.append([unitname,reqnum,attnum,tag])

if file is not None:
    """Opening given file"""
    with open(file) as openfile:
        lines = openfile.readlines()

    """Parsing file for run and project"""
    for l in lines:
        if '#' not in l:
            l = l.split(',')
            unitname,reqnum,attnum = l[0],l[1],l[2]
            if args.tag is None:
                if len(l) < 4:
                    print "Must specify all: unitname,reqnum,attnum,tag!"
                    sys.exit(1)
                tag = l[3]
                run.append([unitname,reqnum,attnum,tag])
            else:
                tag = args.tag
                run.append([unitname,reqnum,attnum,tag])
                
"""Connect to database using user's .desservices file"""
try:
    desdmfile = os.environ["des_services"]
except KeyError:
    desdmfile = None

dbh = DesDbi(desdmfile,section)
cur = dbh.cursor()

""" Add pfw_attempt_id to list """
for list in run:
    unitname,reqnum,attnum,tag = list[0],list[1],list[2],list[3]
    id_query = "select distinct id from pfw_attempt where unitname = '%s' and reqnum='%s' and attnum='%s'" % (unitname,reqnum,attnum)
    cur.execute(id_query)
    id = cur.fetchone()[0]
    list.append(id)

"""For each entry in list, perform an insert"""
for list in run:
    attemptdict = {'unitname':list[0].strip(),'reqnum':list[1].strip(),'attnum':list[2].strip(),'tag':list[3].strip(),'pfw_attempt_id':list[4]}
    query = "insert into PROCTAG (TAG,PFW_ATTEMPT_ID) values ('{tag}',{pfw_attempt_id})".format(
            tag=attemptdict['tag'],pfw_attempt_id=attemptdict['pfw_attempt_id']) 
    if args.update:
        print 'Executing...%s,%s,%s,%s' % (list[0],list[1],list[2],list[3])
        cur.execute(query)
    else:
        print 'Will insert tag = %s for unitname: %s,reqnum: %s,attnum: %s into DB' % (list[3],list[0],list[1],list[2])

if args.update:
    dbh.commit()
cur.close()
dbh.close()
