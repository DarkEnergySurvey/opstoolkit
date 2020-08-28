#! /usr/bin/env python
"""
Take an input list of exposures and create a tag in EXPOSURETAG

Syntax:
    blacklist_exposure.py -e expnum -l list [-c] [-D file_root] [-u] [-a analyst] [-s section] [-S Schema][-v]

    Must have either -e (expnum) or -l file containing a list of eposures 
    numbers (NOTE both can be used).  The list file has the following format.
    The first column has expnum.  It can optionally be followed by a comma 
    in which case the remainder of the line will be used to populute the
    comment field in EXPOSURETAG.   Lines beginning with "#" are ignored.  
    A general comment for all exposures can be give with the -C option 
    (these values are over-ridden by values in the list file).

    Optional outputs:
       -D generates a file with INSERT statements which can be manually inserted in the database.
       -u causes the database update to occur as part of the program execution.

Arguments:
"""
######################################################################################
def ParseCCDlist(in_ccdlist):
    """ Take a comma delimited string listing CCDs and make a list
        - string="All" --> a list from 1-62
        - entries in list may indicate a range by separating numbers with "-"
    """
    if (in_ccdlist == "All"):
        output_list=range(1,63)
    else:
        tmp_ccd_list=in_ccdlist.split(",")
        output_list=[]
        for ccd in tmp_ccd_list:
            if (re.search("-",ccd)is None):
                output_list.append(int(ccd))
            else:
                ccd_subset=ccd.split("-")
                for ccd2 in range(int(ccd_subset[0]),(int(ccd_subset[1])+1)):
                    output_list.append(int(ccd2))
        output_list=sorted(list(set(output_list)))
    return output_list
######################################################################################

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
    import stat
#    import time
    import re
    import sys
#    import datetime
#    import numpy
#    from numpy import *
#    import scipy
    
    svnid="$Id: blacklist_exposure.py 14596 2013-09-13 19:49:56Z rgruendl $"
    dbTable='blacklist'

    parser = argparse.ArgumentParser(description='Add exposures/ccds to the BLACKLIST')
    parser.add_argument('-a', '--analyst',  action='store', type=str, default=None,  help='Provides override value for analyst (default uses os.getlogin())')
    parser.add_argument('-D', '--DB_file',  action='store', type=str, default=None,  help='Flag/argument for optional output of DB update file')
    parser.add_argument('-u', '--updateDB', action='store_true', default=False,      help='Flag for program to DIRECTLY update DB (firstcut_eval).')
    parser.add_argument('-C', '--Comment',  action='store', type=str, default=None,  help='Optional comment/explanation')
    parser.add_argument('-c', '--ccdlist',  action='store', type=str, default=None, help='Optional ccdlist to apply (default=All)')
    parser.add_argument('-e', '--expnum',   action='store', type=str, default=None,  help='Exposure number (or comma delimited list of exposure numbers)')
    parser.add_argument('-l', '--list',     action='store', type=str, default=None,  help='File containing a list of exposure numbers (with optional field for ccd and comments)')
    parser.add_argument('--over_table',     action='store', type=str, default=None,  help='Optional table to update (default=%s)'%(dbTable))
#    parser.add_argument('-t', '--terse',   action='store_true', default=False, help='Flag to give terse summary listing (no individual file information)')
#    parser.add_argument('-q', '--quiet',   action='store_true', default=False, help='Flag to run as silently as possible')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,       help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section', action='store', type=str, default=None,   help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None,   help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

    if ((args.expnum is None)and(args.list is None)):
        parser.print_help()
        exit(1)
#
#   Automatically get analyst name (from os.getlogin()) unless an override value has been specified
#
    if ((args.analyst is None)or(args.analyst == "None")):
        analyst=os.getlogin()
    else:
        analyst=args.analyst

    if ((args.ccdlist is None)or(args.ccdlist == "All")):
        GlobalCCDlist=ParseCCDlist('All')
    else:
        GlobalCCDlist=ParseCCDlist(args.ccdlist)

    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)

    if (args.over_table is None):
        dbTable='%s%s' % (dbSchema,dbTable)
    else:
        if (len(args.over_table.split('.')) > 1):
            dbTable=args.over_table
        else:
            dbTable='%s%s' % (dbSchema,args.over_table)

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
                tmp_exprec["ccdlist"]=GlobalCCDlist
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
                columns=line.split()
                if (len(columns)==0):
                    columns=['#']
                if (columns[0] != "#"):
                    tmp_exprec={}
                    tmp_exprec["expnum"]=int(columns[0])
                    if (len(columns)==1):
                        tmp_exprec["ccdlist"]=GlobalCCDlist
                        if (args.Comment is None):
                            tmp_exprec["comment"]=None
                        else:
                            tmp_exprec["comment"]=args.Comment
                    elif (len(columns)==2):
                        tmp_exprec["ccdlist"]=ParseCCDlist(columns[1].strip())
                        if (args.Comment is None):
                            tmp_exprec["comment"]=None
                        else:
                            tmp_exprec["comment"]=args.Comment
                    elif (len(columns)>2):
#
#                       If whitespace exists (e.g. multiple words form comment) then combine to form a single string.
#
                        tmp_exprec["ccdlist"]=ParseCCDlist(columns[1].strip())
                        tmp_exprec["comment"]=" ".join(columns[2:])
                    expnum_list.append(tmp_exprec)
            f1.close()

    print "Formed exposure list for update: ",len(expnum_list)," exposures found."
#    for exp in expnum_list:
#        print exp["expnum"]
#    print expnum_list

    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section)
    cur = dbh.cursor()

################################################################################################
################################################################################################
#   Two queries: 
#   1) first to find exposure IDs (with a little more info)
#   2) second is to make certain blacklist entries don't already exist 
#

    queryitems1 = ["e.filename", "e.object"] 
    coldict1={}
    for index, item in enumerate(queryitems1):
        coldict1[item]=index
    querylist1 = ",".join(queryitems1)

    queryitems2 = ["b.expnum", "b.ccdnum", "b.reason","b.analyst"]
    coldict2={}
    for index, item in enumerate(queryitems2):
        coldict2[item]=index
    querylist2 = ",".join(queryitems2)

    fullexp_list=[]
    for exprec in expnum_list:  
#
#       Initial exposure ID query used to obtain unique exposure id information that is the same across all CCDs.
#       possibly IMPROVE by taking care of uniqueness in the list within the query?
#
        query1 = """select %s from %sexposure e where e.expnum=%d """ % ( querylist1, dbSchema, exprec["expnum"] )
        if args.verbose:
            print query1
        cur.arraysize = 1000 # get 1000 at a time when fetching
        cur.execute(query1)

        nfound=0
        for item in cur:
            nfound=nfound+1
            tmp_exprec={}
            tmp_exprec["filename"]=item[coldict1["e.filename"]]
            tmp_exprec["expnum"]=exprec["expnum"]   
        if (nfound == 0):
            print("Warning: no record found for exposure with expnum={:d} in exposure.".format(exprec["expnum"]))
        elif (nfound > 1):
            print("Warning: multiple records ({:d}) found for exposure with expnum={:d}.".format(nfound,exprec["expnum"]))
#       for tmprecord in fullexp_list:
#           if (tmprecord["expnum"]==exprec["expnum"]):
#               print("    Found: filename={:s}, expnum={:d} ".format(tmprecord["filename"],tmprecord["expnum"]))

        q_ccdlist=[]
        for ccd in exprec["ccdlist"]:
            q_ccdlist.append(str(ccd))
        q_ccdlist=",".join(q_ccdlist)
            
        query2 = """select %s from %s b where b.expnum=%d and b.ccdnum in (%s) """ % ( querylist2, dbTable, tmp_exprec["expnum"], q_ccdlist)
        if args.verbose:
            print query2
        cur.arraysize = 1000 # get 1000 at a time when fetching
        cur.execute(query2)

        ccdExists=[]
        for item in cur:
            print("Warning: Record already present in {:s}: (expnum={:08d}, CCD={:2d}, analyst={:s}, reason={:s}) ".format(dbTable,int(item[coldict2['b.expnum']]),int(item[coldict2["b.ccdnum"]]),item[coldict2["b.analyst"]],item[coldict2["b.reason"]]))
            ccdExists.append(int(item[coldict2["b.ccdnum"]]))

        if (len(ccdExists)==0):
            tmp_exprec["ccdlist"]=exprec["ccdlist"]
        else:
            q_ccdlist=[]
            for ccd in exprec["ccdlist"]:
                if (not(ccd in ccdExists)):
                    q_ccdlist.append(ccd)
            tmp_exprec["ccdlist"]=q_ccdlist
        tmp_exprec["comment"]=exprec["comment"]
        if (len(tmp_exprec["ccdlist"])>0):
            fullexp_list.append(tmp_exprec)

    print("Number of exposures that will be entered into {:s}: {:d} ".format(dbTable,len(fullexp_list)))
    print("Beginning database update")
################################################
#
#    Prepare for optional DB update file output if args.DBupdate is TRUE
#
    if (not(args.DB_file is None)):
        fdbout=open("%s"% (args.DB_file), 'w')

    InsertCnt=0
    for exprec in fullexp_list:
        for ccd in exprec["ccdlist"]:
            InsertCnt=InsertCnt+1
            db_cname="INSERT INTO %s(EXPNUM,CCDNUM,ANALYST " % dbTable
            db_value=""" VALUES (%d, %d, '%s' """ % (exprec["expnum"],ccd,analyst)
            if (not(exprec["comment"] is None)):
                db_cname=db_cname+','+'REASON'
                db_value=db_value+','+"'%s'" % exprec["comment"]
            db_cname=db_cname+")"
            db_value=db_value+")"
            insert_command=db_cname+db_value
#
#           Write DB_file (or update database) if command line option(s) are present 
#
            PrintIt=True
            if (args.updateDB):
                cur.execute(insert_command)
                PrintIt=False
            if (not(args.DB_file is None)):
                fdbout.write("{:s};\n".format(insert_command))
                PrintIt=False
            if (PrintIt):
                print insert_command

    print "Inserting %d records into %s" % (InsertCnt,dbTable)
    if(args.updateDB):
        dbh.commit()
        print "DB update complete and committed"
    if (not(args.DB_file is None)):
        fdbout.close()
