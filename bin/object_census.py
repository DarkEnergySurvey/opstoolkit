#! /usr/bin/env python
# $Id: verify_tag_objects.py 43197 2016-07-19 19:05:24Z rgruendl $
# $Rev:: 42694                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha

"""
Some simple tools collected under an umbrella to provide information about
objects connected with a PROCTAG.
"""

##########################################
def  check_tag_exists(tag,dbh,dbSchema,verbose=0):
    """Function that simply checks that an entry exists for a specific tag"""

    if (tag is None):
#       Pass through for the case where None is declared.
        TagExists=True
        print("No tag explicitly specified... no check performed")
    else:
#       The "real" check.
        query="""select tag,description
            from {schema:s}ops_proctag_def 
            where tag='{tagname:s}'
        """.format(schema=dbSchema, tagname=tag)

        if (verbose > 0):
            print("# Executing query to check that TAG exists")
            if (verbose == 1):
                print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
            if (verbose > 1):
                print("# sql = {:s}".format(query))
        curDB=dbh.cursor()
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        queryResult=[]
        for row in curDB:
            rowd = dict(zip(desc, row))
            queryResult.append(rowd)
        curDB.close()

        if (len(queryResult)>0):
            TagExists=True
            if (len(queryResult)>1):
                print("# WARNING: Found more that one result (#={:d}) for tag={:s} in {:s}ops_proctag_def".format(len(queryResult),tag,dbSchema))
        else:
            TagExists=False
            print("# WARNING: tag={:s} does not exist in {:s}ops_proctag_def".format(tag,dbSchema))

    return TagExists


##########################################
def generate_attempt_list(ProcTag,ReqNumConstraint,dbh,dbSchema,verbose=0):
    """Function that searchs for a set of attempts that processed a list of exposures"""

    if (ReqNumConstraint is None):
        ConstrainReqNum=""
    else:
        if (ProcTag is None):
            ConstrainReqNum="a.reqnum={:s}".format(ReqNumConstraint)
        else:
            ConstrainReqNum="and a.reqnum={:s}".format(ReqNumConstraint)


    if (ProcTag is None):
        query="""SELECT 
                a.id           as pfw_attempt_id,
                a.archive_path as archive_path,
                a.reqnum       as reqnum,
                a.data_state   as data_state
            FROM {schema:s}pfw_attempt a 
            WHERE {creq:s}
        """.format(schema=dbSchema,ptag=ProcTag,creq=ConstrainReqNum)
    else:
        query="""SELECT 
                a.id           as pfw_attempt_id,
                a.archive_path as archive_path,
                a.reqnum       as reqnum,
                a.data_state   as data_state
            FROM {schema:s}proctag t, {schema:s}pfw_attempt a 
            WHERE t.tag='{ptag:s}'
                and t.pfw_attempt_id=a.id 
                {creq:s}
        """.format(schema=dbSchema,ptag=ProcTag,creq=ConstrainReqNum)

    if (verbose > 0):
        print("# Executing query to obtain list of attempts within a PROCTAG")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB=dbh.cursor()
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    queryResult={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        AID=int(rowd['pfw_attempt_id'])
        if (AID in queryResult):
            print("Warning: multiple entries detected in tag={:s} for PFW_ATTEMPT_ID={:d}.".format(ProcTag,AID))
        queryResult[AID]=rowd
    curDB.close()

    return queryResult


##########################################
def check_num_objects(AttemptDict,dbh,dbSchema,verbose=0):
    """Function that checks that the number of objects in SE_OBJECT match those in CATALOG
       This is a variant that uses a workhorse query that operates one attempt at a time.
    """
    

    FirstQuery=True
    curDB=dbh.cursor()
#,
#                count(o.flux_auto) as num_se_objects 
    queryResult={}
    for key in AttemptDict:
        query="""SELECT 
                c.pfw_attempt_id as pfw_attempt_id,
                c.filename as filename,
                c.objects  as num_objects,
                count(o.flux_auto) as num_se_objects 
            FROM {schema:s}catalog c, {schema:s}se_object o 
            WHERE c.pfw_attempt_id={aid:d} 
                and c.filetype='cat_finalcut'
                and c.filename=o.filename 
                having count(o.flux_auto)!=c.objects 
            GROUP BY c.filename,c.objects
        """.format(schema=dbSchema,aid=AttemptDict[key]['pfw_attempt_id'])

        if (verbose > 0):
            if ((FirstQuery)or(verbose > 2)):
                print("# Executing query to obtain total archive space usage for each attempt")
                if (verbose < 3):
                    print("# Example/first query follows:")
                if (verbose == 1):
                    print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
                if (verbose > 1):
                    print("# sql = {:s}".format(query))
                FirstQuery=False
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

#        FoundProblem=0
        for row in curDB:
#            FoundProblem=FoundProblem+1
            rowd = dict(zip(desc, row))
#            print(" WARNING: query found mismatch for filename={:s} (catalog.objects={:d} NOT EQUAL to count from SE_OBJECT=}".format(
#                rowd['filename'],rowd['num_objects']))
            print(" WARNING: query found mismatch for filename={:s} (catalog.objects={:d} NOT EQUAL to count from SE_OBJECT={:d}".format(
                rowd['filename'],rowd['num_objects'],rowd['num_se_objects']))
#            queryResult[rowd['filename']]={'pfw_attempt_id':AttemptDict[key]['pfw_attempt_id'],'num_objects':rowd['num_objects']}
            queryResult[rowd['filename']]={'pfw_attempt_id':AttemptDict[key]['pfw_attempt_id'],'num_objects':rowd['num_objects'],'num_se_objects':rowd['num_se_objects']}
#        if (FoundProblem < 1):
#            queryResult[AttemptDict[key]['pfw_attempt_id']]="NoProblemo"

    curDB.close()

    return queryResult


##########################################
def check_num_objects_v2(AttemptDict,dbh,dbSchema,verbose=0):
    """Function that checks that the number of objects in SE_OBJECT match those in CATALOG
       This is a variant that uses a query that operates one reqnum at a time.
    """

    queryResult={}
    FirstQuery=True
    curDB=dbh.cursor()

    ReqNumList=[]
    for key in AttemptDict:
        ReqNumList.append(AttemptDict[key]['reqnum'])
   
    UniqReqNumList=sorted(list(set(ReqNumList)))
    if (verbose > 0):
        print("Identified {:d} reqnums ".format(len(UniqReqNumList)))

    for ReqNum in UniqReqNumList:
        IDList=[]
        for key in AttemptDict:
            if (AttemptDict[key]['reqnum'] == ReqNum):
                IDList.append([AttemptDict[key]['pfw_attempt_id']])         

#       Make sure the GTT_ID table is empty
        curDB=dbh.cursor()
        curDB.execute('delete from GTT_ID')
        print("# Loading GTT_ID for secondary queries with entries for {:d} attempts from reqnum={:d}".format(len(IDList),ReqNum))
        dbh.insert_many('GTT_ID',['ID'],IDList)
#       dbh.commit()
#       curDB.execute('select count(*) from gtt_id')
#       for row in curDB:
#           print row 

        t0=time.time()
        query="""SELECT 
                c.pfw_attempt_id as pfw_attempt_id,
                sum(c.objects) as num_objects 
            FROM {schema:s}catalog c, gtt_id gi
            WHERE c.pfw_attempt_id=gi.id
                and c.filetype='cat_finalcut'
            GROUP BY c.pfw_attempt_id
        """.format(schema=dbSchema,rnum=ReqNum)
#
        if (verbose > 0):
            if ((FirstQuery)or(verbose > 2)):
                print("# Executing query to compare total number of catalog entries with number of objects for each attempt")
                if (verbose < 3):
                    print("# Example/first query follows:")
                if (verbose == 1):
                    print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
                if (verbose > 1):
                    print("# sql = {:s}".format(query))
#                FirstQuery=False
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        for row in curDB:
            rowd = dict(zip(desc, row))
            queryResult[rowd['pfw_attempt_id']]=rowd
#
        query="""SELECT 
                c.pfw_attempt_id as pfw_attempt_id,
                count(o.filename) as num_se_objects 
            FROM {schema:s}catalog c, {schema:s}se_object o, gtt_id gi
            WHERE c.pfw_attempt_id=gi.id
                and c.filetype='cat_finalcut'
                and c.filename=o.filename 
            GROUP BY c.pfw_attempt_id
        """.format(schema=dbSchema,rnum=ReqNum)

        if (verbose > 0):
            if ((FirstQuery)or(verbose > 2)):
                print("# Executing query to compare total number of catalog entries with number of objects for each attempt")
                if (verbose < 3):
                    print("# Example/first query follows:")
                if (verbose == 1):
                    print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
                if (verbose > 1):
                    print("# sql = {:s}".format(query))
                FirstQuery=False
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        for row in curDB:
            rowd = dict(zip(desc, row))
            queryResult[rowd['pfw_attempt_id']]['num_se_objects']=rowd['num_se_objects']

        t1=time.time()
        if (verbose > 0):
            print("Subquery over reqnum ({:d}) with {:d} attempts.  Execution time was: {:.2f}".format(ReqNum,len(IDList),(t1-t0)))

    curDB.close()

    return queryResult



##########################################
if __name__ == "__main__":

    import argparse
    import os
    from despydb import DesDbi 
    import stat
    import time
#    import re
    import sys
#    import numpy
    from datetime import datetime

    svnid="$Id: verify_tag_objects.py 43197 2016-07-19 19:05:24Z rgruendl $"

    parser = argparse.ArgumentParser(description='Investigate properties of files within archive for a specific PROCTAG')
    parser.add_argument('--proctag', action='store', type=str, required=True, help='PROCTAG being investigated')
    parser.add_argument('--reqnum',  action='store', type=str, default=None,  help='Constrain search to a specific reqnum')
    parser.add_argument('--checkmerge', action='store_true', default=False,   help='Flag to perform check that merged objects match expected number')
    parser.add_argument('-s', '--section',  action='store', type=str, default=None,  help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',   action='store', type=str, default=None,  help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose',  action='store', type=int, default=0,     help='Print extra (debug) messages to stdout')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

    verbose=args.verbose
#
#   Check for user specified schema
#
    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)

#
#   Check for constraint on ReqNum
#
    if (args.reqnum is None):
        ReqNumConstraint=None
    else:
        ReqNumConstraint=args.reqnum

#
#   Setup DB connection.
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = DesDbi(desdmfile,args.section)
#    cur = dbh.cursor()

################################################################################################
#   Quick check to make sure that a tag exists (if not... then all is for naught)
#

    if (args.proctag == "None"):
        ProcTag=None
    else:
        ProcTag=args.proctag
    if (check_tag_exists(ProcTag,dbh,dbSchema,verbose)):
        if (verbose>0):
            print("Check for existing tag: '{:s}' successfull.".format(args.proctag))
    else:
        print("Aborting!")
        exit(1)

#
#   No matter what go ahead and generate a dict summarizing the attempts in a tag.
#
    t0=time.time()
    AttemptDict=generate_attempt_list(ProcTag,ReqNumConstraint,dbh,dbSchema,verbose)
    t1=time.time()
    if (verbose > 0):
        print("Query to find attempts identified {:d} entries.  Execution time was: {:.2f}".format(len(AttemptDict),(t1-t0)))

#
#   If option given to check that merged objects are present then do so.
#
    if (args.checkmerge):
        t0=time.time()
#        ProblematicDict=check_num_objects(AttemptDict,dbh,dbSchema,verbose)
        ObjectCntDict=check_num_objects_v2(AttemptDict,dbh,dbSchema,verbose)
        t1=time.time()
        if (verbose > 0):
            print("Query to find check each attempts identified {:d} problematic entries.  Execution time was: {:.2f}".format(len(ObjectCntDict),(t1-t0)))

#        print ProblematicDict

        for ID in ObjectCntDict:
            print(" {:12d} {:12s} {:9d} {:9d} ".format(ID,AttemptDict[ID]['data_state'],ObjectCntDict[ID]['num_objects'],ObjectCntDict[ID]['num_se_objects']))

    exit(0)

