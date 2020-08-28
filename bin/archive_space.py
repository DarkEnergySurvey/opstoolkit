#! /usr/bin/env python
# $Id: make_sub_proctag.py 43088 2016-07-12 18:03:45Z rgruendl $
# $Rev:: 42694                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha

"""
Some simple tools collected under an umbrella to provide information about
files connected with a PROCTAG.
"""

##########################################
def  check_tag_exists(tag,dbh,dbSchema,LegacyDB=False,verbose=0):
    """Function that simply checks that an entry exists for a specific tag"""


    if (LegacyDB):
#       For compatibility in Legacy DB
        query="""select tag,description 
            from {schema:s}tag
            where tag='{tagname:s}'
            """.format(schema=dbSchema, tagname=tag)
    else:
#       Current DESDM schema 
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
def generate_tag_list(ProcTag,dbh,dbSchema,verbose=0):
    """Function that searchs for a set of attempts that processed a list of exposures
       For LegacyDB use generate_tag_list_legacy
    """

    query="""SELECT 
        a.id           as pfw_attempt_id,
        a.archive_path as archive_path
        FROM {schema:s}proctag t, {schema:s}pfw_attempt a 
        WHERE t.tag='{ptag:s}'
            and t.pfw_attempt_id=a.id 
        """.format(schema=dbSchema,ptag=ProcTag)

    if (verbose > 0):
        print("# Executing query to obtain paths to attempts within a PROCTAG")
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
def generate_tag_list_legacy(ProcTag,dbh,dbSchema,verbose=0):
    """Function that searchs for a set of attempts that processed a list of exposures
       assuming the legacy DB schema (i.e. runtag, location)

    """

    query="""SELECT 
        run         as run
        FROM {schema:s}runtag t
        WHERE t.tag='{ptag:s}'
        """.format(schema=dbSchema,ptag=ProcTag)

    if (verbose > 0):
        print("# Executing query to obtain paths to attempts within a PROCTAG")
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
        AID=rowd['run']
        if (AID in queryResult):
            print("Warning: multiple entries detected in tag={:s} for RUN={:s}.".format(ProcTag,AID))
        queryResult[AID]=rowd

    curDB.close()

    return queryResult


##########################################
def generate_filespace_list(ProcTag,dbh,dbSchema,verbose=0):
    """Function that finds total filespace in archive for each attempt"""

    query="""SELECT 
        d.pfw_attempt_id  as pfw_attempt_id,
        sum(d.filesize)   as diskspace
        FROM {schema:s}proctag t, {schema:s}desfile d, {schema:s}file_archive_info fai
        WHERE t.tag='{ptag:s}'
            and t.pfw_attempt_id=d.pfw_attempt_id 
            and d.id=fai.desfile_id 
            and fai.archive_name='desar2home'
        GROUP by d.pfw_attempt_id
    """.format(schema=dbSchema,ptag=ProcTag)

    if (verbose > 0):
        print("# Executing query to obtain total archive space usage for each attempt within a PROCTAG")
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
def generate_filespace_list_legacy(ProcTag,dbh,dbSchema,verbose=0):
    """Function that finds total filespace in archive for each attempt"""

#
#   Query to get data and populate structures holding filesizes,filetypes, and runs 
#
    query="""SELECT 
        t.run             as run,
        l.id              as id,
        l.filetype        as filetype,
        l.filesize        as filesize,
        l.filesize_fz     as filesize_fz,
        l.archivesites    as archivesites,
        f.path            as path
        FROM {schema:s}location l, {schema:s}runtag t, filepath f
        WHERE t.tag='{ptag:s}'
            and t.run=l.run 
            and l.id=f.id
    """.format(schema=dbSchema,ptag=ProcTag)

#
#   The following could be added in the query above (for Y1A1_FINALCUT) to make a quick test
#
#            and t.run in ('20140910151316_20130209','20140630112222_20131012','20140718093501_20131212','20140903084451_20140207')

    if (verbose > 0):
        print("# Executing query to obtain total archive space usage for each attempt within a PROCTAG")
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
        AID=int(rowd['id'])
        if (AID in queryResult):
            print("Warning: multiple entries detected in tag={:s} for PFW_ATTEMPT_ID={:d}.".format(ProcTag,AID))
#       Only care about DESDM archive site
        rowd['archivesites']=rowd['archivesites'][-1]
#       Only keep things that exist
        if (rowd['archivesites']!="N"):
            queryResult[AID]=rowd
    curDB.close()

    return queryResult


##########################################
def breakdown_by_run(dspace,verbose=0):
    """Determine statistical breakdown of disk space by run"""
#
#       Determine statistical breakdown
#
    avg_space=numpy.mean(dspace)
    med_space=numpy.median(dspace)
    std_space=numpy.std(dspace)
    if (verbose > 2):
        print("# iterartion, mean, median, stddev")
        print("    {:5d} {:.3f} {:.3f} {:.3f} ".format(0,avg_space,med_space,std_space))

    wsm=numpy.where( dspace > (med_space-10.*std_space) )
    if (len(wsm[0])>0):
        tmp_dspace=dspace[wsm]
        wsm=numpy.where( dspace < (med_space+10.*std_space) )
        if (len(wsm[0])>0):
            tmp_dspace=tmp_dspace[wsm]
            avg_space=numpy.mean(tmp_dspace)
            med_space=numpy.median(tmp_dspace)
            std_space=numpy.std(tmp_dspace)
            if (verbose > 2):
                print("    {:5d} {:.3f} {:.3f} {:.3f} ".format(1,avg_space,med_space,std_space))

            wsm=numpy.where( tmp_dspace > (med_space-5.*std_space) )
            if (len(wsm[0])>0):
                tmp_dspace=tmp_dspace[wsm]
                wsm=numpy.where( tmp_dspace < (med_space+5.*std_space) )
                if (len(wsm[0])>0):
                    tmp_dspace=tmp_dspace[wsm]
                
                avg_space=numpy.mean(tmp_dspace)
                med_space=numpy.median(tmp_dspace)
                std_space=numpy.std(tmp_dspace)
                if (verbose > 2):
                    print("    {:5d} {:.3f} {:.3f} {:.3f} ".format(2,avg_space,med_space,std_space))
#
#       Could show the outliers if wanted
#
#        min5=med_space-(5.0*std_space)
#        max5=med_space+(5.0*std_space)
#        for entry in FileSpaceDict:
#            fspace=FileSpaceDict[entry]['diskspace']/1024./1024.
#            if ((fspace < min5)or(fspace > max5)):
#                print(" Attempt {:d} with diskspace={:.3f}M is a 5-sigma outlier.".format(entry,fspace))
    return



##########################################
if __name__ == "__main__":

    import argparse
    import os
    from despydb import DesDbi 
    import stat
    import time
    import re
    import sys
    import numpy
    from datetime import datetime

    svnid="$Id: archive_space.py 43088 2016-07-12 18:03:45Z rgruendl $"

    parser = argparse.ArgumentParser(description='Investigate properties of files within archive for a specific PROCTAG')
    parser.add_argument('--proctag', action='store', type=str, required=True, help='PROCTAG being investigated')
    parser.add_argument('--dlist',   action='store', type=str, default=None,  help='Write file containing a list of base directories associated with each PFW_ATTEMPT_ID (a value of STDOUT writes to STDOUT)')
    parser.add_argument('--dspace',  action='store_true', default=False,      help='Flag for program to probe disk usage')
    parser.add_argument('--dtype',   action='store_true', default=False,      help='Flag for program to probe disk usage (with breakdown by filetype')
    parser.add_argument('--legacy',  action='store_true', default=False,      help='Flag that tag is part of the legacy schema (e.g. SVA/Y1A1)')
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

    if (check_tag_exists(args.proctag,dbh,dbSchema,args.legacy,verbose)):
        if (verbose>0):
            print("Check for existing tag: '{:s}' successfull.".format(args.proctag))
    else:
        print("Aborting!")
        exit(1)

#
#   No matter what go ahead and generate a dict summarizing the attempts in a tag.
#
    t0=time.time()
    if (args.legacy): 
        TagDict=generate_tag_list_legacy(args.proctag,dbh,dbSchema,verbose)
    else:
        TagDict=generate_tag_list(args.proctag,dbh,dbSchema,verbose)
    t1=time.time()
    if (verbose > 0):
        print("Query to find attempts identified {:d} entries.  Execution time was: {:.2f}".format(len(TagDict),(t1-t0)))

#
#   If option given to generate a directory listing for the tag then do so.
#
    if (args.dlist is not None):
        #  If working with the legacy DB this case requires a further query (to assemble the base path for each run).
        if (args.legacy):
            t0=time.time()
            FileSpaceDict=generate_filespace_list_legacy(args.proctag,dbh,dbSchema,verbose)
            t1=time.time()
            if (verbose > 0):
                print("Query to find files identified {:d} entries.  Execution time was: {:.2f}".format(len(FileSpaceDict),(t1-t0)))

            all_ftype=[]
            for run in TagDict:
                run_space=[FileSpaceDict[k] for k in FileSpaceDict if (FileSpaceDict[k]['run'] == run) ]
                ftype_list=[obj['filetype'] for obj in run_space]

                total_fspace=0.0
                total_fcnt=0
                cpath_all=[]
                fspace_dict={}
                for ftype in sorted(list(set(ftype_list))):
#                   Keep a running list of all ftypes for later
                    all_ftype.append(ftype)
                    path_list=[obj['path'] for obj in run_space if (obj['filetype']==ftype) ]
                    fcnt=len(path_list)
                    common_path=os.path.commonprefix(path_list).rpartition('/')[0]
                    fspace_list1=numpy.array([obj['filesize_fz']/1024./1024. for obj in run_space if ((obj['filetype']==ftype)and(obj['archivesites']=='F')) ])
                    fspace_list2=numpy.array([obj['filesize']/1024./1024. for obj in run_space if ((obj['filetype']==ftype)and(obj['archivesites']=='Y')) ])
                    fspace=numpy.sum(fspace_list1)+numpy.sum(fspace_list2)

                    fspace_dict[ftype]={}
                    fspace_dict[ftype]={'dspace':fspace,'cpath':common_path,'fcnt':fcnt} 

                    total_fspace=total_fspace+fspace
                    total_fcnt=total_fcnt+fcnt
                    cpath_all.append(common_path)

                common_path=os.path.commonprefix(cpath_all).rpartition('/')[0]
                TagDict[run]['diskspace']=total_fspace
                TagDict[run]['fcnt']=total_fcnt
                TagDict[run]['archive_path']=common_path
                TagDict[run]['diskusage']=fspace_dict 
#           Replace running list with sorted list of unique filetypes
            all_ftype=sorted(list(set(all_ftype)))

        # Special case to allow writing to STDOUT
        if (args.dlist.upper() == "STDOUT"):
            sys.stdout.flush()
            fout=sys.stdout
        else:
            fout=open(args.dlist,"w")

        for entry in TagDict:
            fout.write("{:s}\n".format(TagDict[entry]['archive_path']))

        if (fout is not sys.stdout):
            fout.close()
#
#       Not so nice for the Storage Condo but could get same info by directly probing the file system
#
#        for entry in TagDict:
#            t0=time.time()
#            fpath=os.path.join('/archive_data/desarchive',TagDict[entry]['archive_path'])
#           
#            total_size = 0
#            fcnt=0
#            for dirpath, dirnames, filenames in os.walk(fpath):
#                for f in filenames:
#                    fcnt=fcnt+1
#                    fp = os.path.join(dirpath, f)
#                    total_size += os.path.getsize(fp)
#            TagDict[entry]['filecount']=fcnt
#            TagDict[entry]['diskspace']=total_size
#
#        t1=time.time()
#        if (verbose > 0):
#            print("OS Walk method over {:d} entries.  Execution time was: {:.2f}".format(len(TagDict),(t1-t0)))

#
#   Now more specific queries/functions.
#   Total Filespace:
#
    if (args.dspace):
        if (not(args.legacy)):
            # If this were a query for legacy data it was already handled in the previous condition
            t0=time.time()
            FileSpaceDict=generate_filespace_list(args.proctag,dbh,dbSchema,verbose)
            t1=time.time()
            if (verbose > 0):
                print("Query to find diskspace associated with each attempt identified {:d} entries.  Execution time was: {:.2f}".format(len(TagDict),(t1-t0)))
#
#          Cross-check(s): see that an entry was found for all entries in the original TagDict
#
            for entry in FileSpaceDict:
                if (entry not in TagDict):
                    print("Warning: EXTREMELY ODD to find a set of files for PFW_ATTEMPT_ID={:d} without an entry from the general tag query".format(entry))
            for entry in TagDict:
                if (entry in FileSpaceDict):
                    if (int(FileSpaceDict[entry]['diskspace'])==0):
                        print("Cross-check shows DB has ZERO diskspace associated with PFW_ATTEMPT_ID={:d}".format(entry))
                else:
                    print("Cross-check shows no files were associated with PFW_ATTEMPT_ID={:d}".format(entry))
                    FileSpaceDict[entry]={}
                    FileSpaceDict[entry]=0

#
#           Update the TagDict with information about the total disk usage 
#           (This is both Legacy and current versions generate similar format)
#
            for entry in TagDict:
                if (entry in FileSpaceDict):
                    TagDict[entry]['diskspace']=FileSpaceDict[entry]['diskspace']/1024./1024.
#
#       Now go ahead and write the desired information about total disk usage
#

        dspace=numpy.array([TagDict[k]['diskspace'] for k in TagDict])
        sum_dspace=numpy.sum(dspace)
        print("Total disk usage in GB: {:.3f} ".format(sum_dspace/1024.))
        breakdown_by_run(dspace,verbose)

    if (args.dtype):
        if (args.legacy):
            print("# ")
            print("# Breakdown by filetype:")
            for ftype in all_ftype:
                dspace=numpy.array([TagDict[run]['diskusage'][ftype]['dspace'] for run in TagDict if (ftype in TagDict[run]['diskusage'])])
#                print dspace
                sum_dspace=numpy.sum(dspace)
                print(" {:>30s} {:.3f} (GB) ".format(ftype,sum_dspace/1024.))
        else:
            print("Breakdown by filetype currently only supported under legacy system")


    exit(0)

