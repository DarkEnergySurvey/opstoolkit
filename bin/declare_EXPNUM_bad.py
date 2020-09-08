#! /usr/bin/env python3
"""
declare_EXPNUM_bad.py
v1                RAG 2016-09-19
python3 migration RAG 2020-09-08

Create/write database entries for unprocessable exposures so that they
can be declared bad despite not receiving full first cut processing.

Must have either -e (expnum) or -l file containing a list of eposures 
numbers (NOTE both can be used).  The list file has the following format.
The first column has expnum.  It can optionally be followed by a comma 
in which case the remainder of the line will be used to populute the
comment field in FIRSTCUT_EVAL.   Lines beginning with "#" are ignored.  
A general comment for all exposures can be give with the -C option 
(these values are over-ridden by values in the list file).

Optional outputs:
  -D generates a file with INSERT statements which can be manually inserted in the database.
  -u causes the database update to occur as part of the program execution.
  -c changes the value that will be forced/used for CAMSYM (the default is 'D')

Arguments:
     
"""

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
    import stat
    import time
    import re
#    import csv
    import sys
    from datetime import datetime

    db_table="firstcut_eval"
    svnid="$Id: declare_EXPNUM_bad.py 43737 2016-08-19 15:29:04Z rgruendl $"

    parser = argparse.ArgumentParser(description='Declare an exposure number bad (unable to process or render an automated assessment).')
    parser.add_argument('-a', '--analyst',  action='store', type=str, default=None, help='Provides override value for analyst (default uses os.getlogin())')
    parser.add_argument('-u', '--updateDB', action='store_true', default=False,     help='Flag for program to DIRECTLY update DB (firstcut_eval).')
    parser.add_argument('-D', '--DB_file',  action='store', type=str, default=None, help='Optional output of DB update file')
    parser.add_argument('-C', '--Comment',  action='store', type=str, default=None, help='Optional comment/explanation')
    parser.add_argument('-e', '--expnum',   action='store', type=str, default=None, help='Exposure number (or comma delimited list of exposure numbers)')
    parser.add_argument('-l', '--list',     action='store', type=str, default=None, help='File containing a list of exposure numbers (with optional second field for comments')
    parser.add_argument('--AttID',          action='store', type=str, default=None,  help='Specify Attempt Id that showed this exposure to be BAD')
#    parser.add_argument('--ReqNum',   action='store', type=str, default=None,  help='Specify ReqNum from processing attempt that showed this exposure to be BAD')
#    parser.add_argument('--UnitName', action='store', type=str, default=None,  help='Specify UnitName from processing attempt that showed this exposure to be BAD')
#    parser.add_argument('--AttNum',   action='store', type=str, default=None,  help='Specify AttNum from processing attempt that showed this exposure to be BAD')
    parser.add_argument('--good',     action='store_true', default=False, help='Flag to declare all exposures GOOD (i.e. accepted=True) rather than bad')
    parser.add_argument('-c', '--camsym',   action='store', type=str, default='D',  help='Optional/override value for CAMSYM in DB queries (default=\'D\')')
    parser.add_argument('--over_table',     action='store', type=str, default=None, help='Override output DB table with specified table')
#    parser.add_argument('-f', '--froot',    action='store', type=str, default=None, help='Root for output file names')
    parser.add_argument('-v', '--verbose',  action='store_true', default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section',  action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',   action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)

    if ((args.expnum is None)and(args.list is None)):
        print(" ")
        print("ERROR: Must provide one (or both) of the following.")
        print(" 1) an expnum or list of expnum (-e), or ")
        print(" 2) a file containg a list of exposure numbers (-l).")
        print("Aborting!")
        print(" ")
        parser.print_help()
        exit(1)

#
#   Check for user define DB schema and whether DB table is being over ridden
#
    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

    if (args.over_table is None):
        db_table='%s%s' % (db_Schema,db_table)
    else:
        if (len(args.over_table.split('.')) > 1):
            db_table=args.over_table
        else:
            db_table='%s%s' % (db_Schema,args.over_table)

#
#   If an optional DB_file (containing INSERT commands) is chosen the set flag...
#
    if (args.DB_file is None):
        use_DB_file=False
    else:
        use_DB_file=True

#
#   Automatically get analyst name (from os.getlogin()) unless an override value has been specified
#
    if ((args.analyst is None)or(args.analyst == "None")):
        analyst=os.getlogin()
    else:
        analyst=args.analyst

#
#   populate the expnum_list with values
#
    expnum_list=[]
    if (not(args.expnum is None)):
        tmp_list=args.expnum.split(',')
        for tmp_entry in tmp_list:
            if (tmp_entry.strip() != ''):
                tmp_exprec={}
                tmp_exprec["expnum"]=int(tmp_entry.strip())
                if (not(args.Comment is None)):
                    tmp_exprec["comment"]=args.Comment
                else:
                    tmp_exprec["comment"]=None
                expnum_list.append(tmp_exprec)

    if (not(args.list is None)):
        if os.path.isfile(args.list):
            f1=open(args.list,'r')
            for line in f1:
                line=line.strip()
                columns=line.split(',')
                if (columns[0] != "#"):
                    tmp_exprec={}
                    tmp_exprec["expnum"]=int(columns[0])
                    if (len(columns)>1):
                        tmp_exprec["comment"]=columns[1].strip()
                    elif (not(args.Comment is None)):
                        tmp_exprec["comment"]=args.Comment
                    else:
                        tmp_exprec["comment"]=None
                    expnum_list.append(tmp_exprec)
            f1.close()

    print("Formed exposure list for processing: {:d} exposures found.".format(len(expnum_list)))
#    print(expnum_list)

    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydy.desdbi.DesDbi(desdmfile,args.section,retry=True)
    cur = dbh.cursor()

################################################################################################
################################################################################################
#  
#   Initial exposure ID query used to obtain unique exposure id information that is the same across all CCDs.
#   possibly IMPROVE by taking care of uniqueness in the list within the query?
#

    queryitems = ["e.filename", "e.program", "e.obstype", "e.object"] 
    coldict={}
    for index, item in enumerate(queryitems):
        coldict[item]=index
    querylist = ",".join(queryitems)

    fullexp_list=[]

    for exprec in expnum_list:  
        query = """select %s from %sexposure e where e.expnum=%d and e.camsym='%s' """ % ( querylist, db_Schema, exprec["expnum"], args.camsym  )
    
        if args.verbose:
            print(query)
        cur.arraysize = 1000 # get 1000 at a time when fetching
        cur.execute(query)

        nfound=0
        for item in cur:
            nfound=nfound+1
            tmp_exprec={}
#           tmp_exprec["id"]=item[coldict["e.id"]]
            tmp_exprec["fname"]=item[coldict["e.filename"]]
            tmp_exprec["obstype"]=item[coldict["e.obstype"]]
            tmp_exprec["expnum"]=exprec["expnum"]   
            tmp_exprec["comment"]=exprec["comment"] 
#
#           Logic to determine whether an exposure belongs to the SN or Survey programs.
#           And then treat appropriately (keep/remove from further consideration)
#
            if (item[coldict["e.program"]]=="survey"):
                tmp_exprec["program"]='survey'
                tmp_exprec["sn_field"]=False
                tmp_exprec["survey_field"]=True
            elif (item[coldict["e.program"]]=="supernova"):
                tmp_exprec["program"]='SN'
                tmp_exprec["sn_field"]=True
                tmp_exprec["survey_field"]=False
            elif (item[coldict["e.program"]]=="photom-std-field"):
                tmp_exprec["program"]='phot-std'
                tmp_exprec["sn_field"]=False
                tmp_exprec["survey_field"]=False
            else:
                if (tmp_exprec["obstype"] in ['zero','dark','dome flat','sky flat']):
                    tmp_exprec["program"]='cal'
                    tmp_exprec["sn_field"]=False
                    tmp_exprec["survey_field"]=False
                else:
                    tmp_exprec["program"]='unknown'
                    tmp_exprec["sn_field"]=False
                    tmp_exprec["survey_field"]=False

            fullexp_list.append(tmp_exprec)

        if (nfound == 0):
            print("Warning: no record found for exposure with expnum={:d} in exposure.".format(exprec["expnum"]))
        elif (nfound > 1):
            print("Warning: multiple records ({:d}) found for exposure with expnum={:d}.".format(nfound,exprec["expnum"]))
            for tmprecord in fullexp_list:
                if (tmprecord["expnum"]==exprec["expnum"]):
                    print("    Found: exposurename={:s}, expnum={:d} ".format(tmprecord["fname"],tmprecord["expnum"]))

    print("Number of exposures found: {0:d} ".format(len(fullexp_list)))
    if (args.updateDB):
        print("Beginning database update")
    else:
        print("No direct database update requested...")

#
#   Prepare for optional DB update file output if use_DB_file is TRUE
#
    if (use_DB_file):
        fdbout=open(args.DB_file, 'w')
        print("Writing results to file: {:s}".format(args.DB_file))


################################################
#   Write out records to appropriate places.

    num_insert=0
    for exp_rec in fullexp_list:
        dm_process="True"
        if (args.good):
            dm_accept="True"
        else:
            dm_accept="False"
#
#       Write DB_file (or update database) if command line option(s) are present 
#
        if((args.updateDB)or(use_DB_file)):
            prog_name='Unknown'
            if (exp_rec["sn_field"]):
                prog_name='supernova'
            if (exp_rec["survey_field"]):
                prog_name='survey'

            db_cname="INSERT INTO %s(EXPOSURENAME,EXPNUM,CAMSYM,PROCESSED,ACCEPTED,PROGRAM,ANALYST,LASTCHANGED_TIME" % ( db_table )
            db_value=""" VALUES ('%s', %d, '%s', '%s', '%s', '%s', '%s', sysdate """ % (exp_rec["fname"],exp_rec["expnum"],args.camsym,dm_process,dm_accept,prog_name,analyst)
#
#           Optional column entries
#
#            if (not(args.ReqNum is None)):
#                db_cname=db_cname+','+'REQNUM'
#                db_value=db_value+','+"%d" % int(args.ReqNum)
#            if (not(args.AttNum is None)):
#                db_cname=db_cname+','+'ATTNUM'
#                db_value=db_value+','+"%d" % int(args.AttNum)
#            if (not(args.UnitName is None)):
#                db_cname=db_cname+','+'UNITNAME'
#                db_value=db_value+','+"'%s'" % args.UnitName
            if (not(args.AttID is None)):
                db_cname=db_cname+','+'PFW_ATTEMPT_ID'
                db_value=db_value+','+"'%s'" % args.AttID
            if (not(exp_rec["comment"] is None)):
                db_cname=db_cname+','+'ANALYST_COMMENT'
                db_value=db_value+','+"'%s'" % exp_rec["comment"]
#
#           Finsihed adding to INSERT command...
#
            db_cname=db_cname+")"
            db_value=db_value+")"

            insert_command=db_cname+db_value

            num_insert=num_insert+1
            if (args.verbose):
                if (num_insert == 1):
                    print(db_cname)
                print(db_value)


            if(args.updateDB):
                cur.execute(insert_command)
            if (use_DB_file):
                fdbout.write("{:s};\n".format(insert_command))
#            if (args.verbose):
#                print("%s".format(insert_command))

    if(args.updateDB):
        dbh.commit()
        print("DB update complete and committed")
    if (use_DB_file):
        fdbout.close()
    exit(0)
