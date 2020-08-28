#! /usr/bin/env python

"""
Created by Michael D. Johnson
File takes an sql query on the command line and either prints the results to screen or it prints it to a line-separated file"""

""" Importing necessary python modulesi"""
from cx_Oracle import *
import os
import re
import argparse
from despydb import DesDbi

""" Creating command line arguments"""
parser=argparse.ArgumentParser()
parser.add_argument('--query','-q', help='Wrap SQL query in "". No need to end in ;')
parser.add_argument('--filename','-f',help='Name of output file to which query is written. File saved by default to cwd.')
parser.add_argument('-p', action='store_true',default=False,dest='p',help="When specified query results won't print to file but will print to screen")
parser.add_argument('--section','-s',help='DB in desservices.py file')
args=parser.parse_args()
query=args.query
filename=args.filename
section= args.section

""" Searching for .desdm file in users home directory, parse the file, and connect to the appropriate database"""
try:
    desdmfile =  os.environ["des_services"]
except KeyError:
    desdmfile  = None

dbh = DesDbi(desdmfile,section)
cur = dbh.cursor()

""" Querying the database and extract data from query"""
cur.execute(query)
lines=cur.fetchall()

if args.p == False:
    """ Writing extracted data to file and formatting it to separate results by line"""
    f=open(filename,'w')
    line = '### %s' % query
    f.write(line)
    f.write('\n')
    listy=[]
    for i,tuple in enumerate(lines):
        lol=list(tuple)
        listy.append(lol)
    for j in listy:
        j=str(j).strip("[]")
        j=re.sub("'",'',j)
        f.write(j)
        f.write('\n')
    f.close()
else:
    listy=[]
    for i,tuple in enumerate(lines):
        lol=list(tuple)
        listy.append(lol)
    for j in listy:
        j=str(j).strip("[]")
        j=re.sub("'",'',j)
        print j

""" Closing the connection to the database"""
cur.close()
dbh.close()
