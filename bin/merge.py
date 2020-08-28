#!/usr/bin/env python
import sys
from despydb import DesDbi
import subprocess
import datetime

reqnum = sys.argv[2]
section = sys.argv[1]
main_obj_table = sys.argv[3]

dbh = DesDbi(None, section)
cur = dbh.cursor()

query = "select distinct reqnum from se_object where reqnum=%s" %reqnum
cur.execute(query)
results = cur.fetchall()
if len(results)==0:
    print datetime.datetime.now()
    cmd = "merge_objects.py -targettable=%s -request=%s" %(main_obj_table, reqnum)
    print "Executing %s" %cmd
    command = subprocess.call(cmd, shell = True)
    print datetime.datetime.now()
else:
    print "Reqnum %s exists in the se_object table!" %reqnum
    sys.exit()
