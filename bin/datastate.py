#! /usr/bin/env python3

"""
Change the data management state for processing attempts in the pfw_attempt table. Can update one attempt at a time given commandline options or specify a file of line-separated attempts. Example of file below:

Example list: 
#unitname,reqnum,attnum,datastate
D00155739,70,01,JUNK
D00155742,70,02,ACTIVE
D00205550,71,01,ACTIVE


Created by Michael D. Johnson. August, 6th 2014
Python3 Migration by RAG, September 9th, 2020

For usage information: ./datastate.py -h

"""


import os
import sys

import argparse

#from despydb import DesDbi
import despydb.desdbi 

"""Create command line arguments"""
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--newstate','-n',help='New state for specified data')
parser.add_argument('--reqnum','-r',
                    help='A single run or list of runs separated by a comma')
parser.add_argument('--attnum','-a',help='Attempt number')
parser.add_argument('--unitname','-u',help='Exposure number, e.g., D00239456')
parser.add_argument('--section','-s',help='The db to connect to through the .desservices file. Defaults to db-desoper')
parser.add_argument('--file','-f',help='File of line separated runs')
parser.add_argument('--dbupdate','-d',action='store_true',default=False,help='If specified code will update the table')
args = parser.parse_args()

file = args.file
section = args.section

run = []
"""Determine if user gave a file or a command-line list"""
if file is None:
    """Split the input runs into a list if user specifies multiple runs."""
    unitname = args.unitname
    reqnum = args.reqnum
    attnum = args.attnum
    newstate = args.newstate
    if unitname is None or reqnum is None or attnum is None or newstate is None:
        print("Must specify all: unitname,reqnum,attnum,newstate!")
        sys.exit(1)
    run.append([unitname,reqnum,attnum,newstate])

if file is not None:
    """Opening given file"""
    openfile = open(file)
    lines = openfile.readlines()
    openfile.close()

    """Parsing file for run and project"""
    for l in lines:
        if '#' not in l:
            l = l.split(',')
            if len(l) < 4:
                print("Must specify all: unitname,reqnum,attnum,newstate!")
                sys.exit(1)
            unitname = l[0]
            reqnum = l[1]
            attnum = l[2]
            if args.newstate:
                newstate = args.newstate
            else:
                newstate = l[3]
            run.append([unitname,reqnum,attnum,newstate])

"""Connect to database using user's .desservices file"""
try:
    desdmfile = os.environ["des_services"]
except KeyError:
    desdmfile = None

dbh = despydb.desdbi.DesDbi(desdmfile,section,retry=True)
cur = dbh.cursor()

if args.dbupdate is False:
    for r in run:
        quickcheck= "select a.data_state from attempt_state a ,pfw_attempt p where p.id=a.pfw_attempt_id and reqnum='%s' and unitname='%s' and attnum='%s'" % (r[1],r[0],r[2])
        cur.execute(quickcheck)
        originalq = cur.fetchall()
        if len(originalq) == 0:
            print('{:s} (r{:s},p{:s}) {:s}'.format('Attempt',r[0],r[1],r[2],' not in database!'))
        for runs in originalq:
            originalstate = originalq[0]
            print('For unitname  {:s}, reqnum = {:s} and attnum = {:s}, Current state = {:s}'.format(r[0],r[1],r[2],originalstate[0]))
    cur.close()
    dbh.close()        
    sys.exit(1)
        
if args.dbupdate is True:

    """Grab list of allowable states and save it in a list for 
        comparison with user input
    """
    cur.execute("select distinct state from OPS_DATA_STATE_DEF")
    stateslines = cur.fetchall()
    data_states = []
    for i,lines in enumerate(stateslines):
        ds = stateslines[i][0]
        data_states.append(ds)
    ### ALLOWABLE STATES SHOULD BE NEW,ACTIVE,JUNK,ARCHIVED    

    """Exit if given state is not an allowable state. 
    Update value and commit to database if given state
    is allowable.

    """
    #if newstate.upper() not in data_states:
    #    print '%s %s%s' % ('Not an allowable state!!!',
    #                       'Allowable states are: ',data_states)
    #    sys.exit(1)
    #else:
    """Check to see if specified runs are in the database. 
       Remove run from list if not in database. Then check to
       see for the remaining runs if the project is the
       correct one.
    """
    runlist = []
    for r in run:
        runlist.append(r)
        q = "select count(*) from pfw_attempt where unitname='%s' and reqnum = '%s' and attnum='%s'" % (r[0],r[1],r[2])
        cur.execute(q)
        ret = cur.fetchone()
        if ret == 0:
            print('{:s} (r{:s},{:s},p{:s}) {:s}'.format('Attempt',r[0],r[1],r[2],' not in database!'))
            runlist.remove(r)
    
    """sql query to update db"""
    for run in runlist:
        attemptdict = {'unitname':run[0].strip(),'reqnum':run[1].strip(),'attnum':run[2].strip(),'newstate':run[3].strip()}
        attid = "select distinct id from pfw_attempt where unitname = '{unitname}' and \
                reqnum = {reqnum} and \
                attnum = {attnum}".format(
                unitname=attemptdict['unitname'],
                reqnum=attemptdict['reqnum'],
                attnum=attemptdict['attnum'])
        cur.execute(attid)
        pfw_attempt_id = cur.fetchone()[0]
        updatestate = "update attempt_state set \
                       data_state = '{newstate}' where pfw_attempt_id = {attid}".format(
                       attid=pfw_attempt_id,newstate = newstate)
        cur.execute(updatestate)
        print("Updated {:s},r{:s},p{:s},{:d}. New state = {:s}.".format(run[0],run[1],run[2],pfw_attempt_id,newstate))
    dbh.commit()
    
cur.close()
dbh.close()
