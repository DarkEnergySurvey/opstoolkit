#! /usr/bin/env python3
"""
Take an input list of exposures and create a tag in EXPOSURETAG

Syntax:
    add_exptag.py -e expnum -l list [-D file_root] [-u] 
                  [-a analyst] [-s section] [-v]

    Must have either -e (expnum) or -l file containing a list of eposures 
    numbers (NOTE both can be used).  The list file has the following format.
    The first column has expnum.  It can optionally be followed by a comma 
    in which case the remainder of the line will be used to populute the
    REASON field in EXPOSURETAG.   Lines beginning with "#" are ignored.  
    A general comment for all exposures can be give with the -C option 
    (these values are over-ridden by values in the list file).

    Optional outputs:
       -D generates a file with INSERT statements which can be manually inserted in the database.
       -u causes the database update to occur as part of the program execution.

    Notes on the current version (from RAG).
        - this was quickly hacked from a personal version used in the Legacy system
        - so there are not a lot of safeguards (checking to make sure DB prohibits corner cases)
            - no check to stop multiple/duplicate records (Parent key might be good enough already)
            - no check to stop (only warning) if multiple records are present in EXPOSURE
        - use of two lists of dictionaries is left over from old code... kind of klunky (expnum_list, full_explist)


Arguments:
     
"""

if __name__ == "__main__":

    import argparse
    import despydb.desdbi
    import os
    import stat
    import time
    import re
    import csv
    import sys
    import datetime
    
    svnid="$Id: add_exptag.py 40166 2015-09-22 21:24:45Z rgruendl $"
    parser = argparse.ArgumentParser(description='Add an exposure or a list of exposures to a TAG within EXPOSURETAG')
    parser.add_argument('--analyst', '-a',  action='store', type=str, default=None,  help='Provides override value for analyst (default uses os.getlogin())')
    parser.add_argument('--updateDB', '-u', action='store_true',      default=False, help='Flag for program to DIRECTLY update DB (firstcut_eval).')
    parser.add_argument('--DB_file',  '-D', action='store', type=str, default=None,  help='Optional output of DB update file')
    parser.add_argument('--Comment',  '-C', action='store', type=str, default=None,  help='Optional comment/explanation')
    parser.add_argument('--expnum',   '-e', action='store', type=str, default=None,  help='Exposure number (or comma delimited list of exposure numbers)')
    parser.add_argument('--list',     '-l', action='store', type=str, default=None,  help='File containing a list of exposure numbers (with optional second field for comments')
    parser.add_argument('--tag',      '-t', action='store', type=str, default=None,  help="Name of tag to apply")
    parser.add_argument('--camsym',   '-c', action='store', type=str, default='D',   help='Optional/override value for CAMSYM in DB queries (default=\'D\')')
    parser.add_argument('--verbose',  '-v', action='store_true',      default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('--section',  '-s', action='store', type=str, default=None,  help='Section of .desservices file with connection info')
    parser.add_argument('--Schema',   '-S', action='store', type=str, default=None,  help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)


    if ((args.expnum is None)and(args.list is None)):
        print("Must have either --expnum or/and --list arguments")
        parser.print_help()
        exit(1)
    if (args.tag is None):
        print("Must have --tag argument")
        parser.print_help()
        exit(1)

#
#   Set schema 
#
    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)
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

    print("Formed exposure list for update: {:d} exposures found.".format(len(expnum_list)))
#    print(expnum_list)

    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
    cur = dbh.cursor()

#
#   Check that TAG is defined (in EXPOSURETAG_DEF)
#
    query = """select td.tag,td.description from %sexposuretag_def td where td.tag='%s' """ % ( db_Schema, args.tag )
    cur.execute(query)
    
    nfound=0
    for item in cur:
        print(item)
        nfound=nfound+1
        if (args.verbose):
            print("Exposure tag {:s} found!".format(args.tag))
            print("Description: {:s} ".format(item[1]))

    if (nfound != 1):
        if (nfound == 0):
            print("No tag, {:s}, found in {:s}EXPOSURETAG_DEF".format(args.tag,db_Schema))
            print("Aborting!")
            exit(1)
        elif (nfound > 1):
            print("Multiple tags, {:s}, found in {:s}EXPOSURETAG_DEF? ".format(args.tag, db_Schema))
            print("Aborting!")
            exit(1)

    DBtable='exposuretag'
################################################################################################
################################################################################################
#  
#   Initial exposure query used to obtain unique exposure filename information that is the same across all CCDs.
#   possibly IMPROVE by taking care of uniqueness in the list within the query?
#

    queryitems = ["e.filename", "e.camsym"] 
    coldict={}
    for index, item in enumerate(queryitems):
        coldict[item]=index
    querylist = ",".join(queryitems)

    fullexp_list=[]

    for exprec in expnum_list:  
        query = """select %s from %sexposure e where e.expnum=%d and e.camsym='%s' """ % ( querylist, db_Schema, exprec["expnum"], args.camsym )
    
        if args.verbose:
            print(query)
        cur.arraysize = 1000 # get 1000 at a time when fetching
        cur.execute(query)

        nfound=0
        for item in cur:
            nfound=nfound+1
            tmp_exprec={}
            tmp_exprec["expnum"]=exprec["expnum"]   
            tmp_exprec["name"]=item[coldict["e.filename"]]
            tmp_exprec["camsym"]=item[coldict["e.camsym"]]
            tmp_exprec["comment"]=exprec["comment"] 
            fullexp_list.append(tmp_exprec)

        if (nfound == 0):
            print("Warning: no record found for exposure with expnum={:d} in exposure.".format(exprec["expnum"]))
        elif (nfound > 1):
            print("Warning: multiple records ({:d}) found for exposure with expnum={:d}.".format(nfound,exprec["expnum"]))
            for tmprecord in fullexp_list:
                if (tmprecord["expnum"]==exprec["expnum"]):
                    print("    Found: expid={:d}, exposurename={:s}, expnum={:d} ".format(tmprecord["id"],tmprecord["name"],tmprecord["expnum"]))

    print("Number of exposures found: {0:d} ".format(len(fullexp_list)))
    print("Beginning database update")
#
#    Prepare for optional DB update file output if args.DBupdate is TRUE
#
    if (args.DB_file is not None):
        fdbout=open("%s"% (args.DB_file), 'w')

################################################
#   Write out records to appropriate places.

    for exp_rec in fullexp_list:
#
#       Write DB_file (or update database) if command line option(s) are present 
#
        if((args.updateDB)or(args.DB_file is not None)):

            db_cname="INSERT INTO %s%s(CAMSYM,EXPNUM,TAG,ANALYST,CREATED_DATE " % (db_Schema,DBtable)
            db_value=""" VALUES ('%s', %d, '%s', '%s', sysdate """ % (args.camsym,exp_rec["expnum"],args.tag,analyst)
            if (not(exp_rec["comment"] is None)):
                db_cname=db_cname+','+'REASON'
                db_value=db_value+','+"'%s'" % exp_rec["comment"]
            db_cname=db_cname+")"
            db_value=db_value+")"

            insert_command=db_cname+db_value

            if(args.updateDB):
                try:
                    cur.execute(insert_command)
                except Exception as e:  
                    print("   For exposure {:d} ".format(exp_rec['expnum']))
                    print("   ",e)
                    print("   Aborting without any commits!")
                    exit(1)
                    
            if (args.DB_file is not None):
                fdbout.write("{:s};\n".format(insert_command))

    if(args.updateDB):
        dbh.commit()
        print("DB update complete and committed")
    if (args.DB_file):
        fdbout.close()
