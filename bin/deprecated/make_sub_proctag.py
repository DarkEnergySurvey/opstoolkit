#! /usr/bin/env python3
# $Id: make_sub_proctag.py 43088 2016-07-12 18:03:45Z rgruendl $
# $Rev:: 42694                            $:  # Revision of last commit.
# $LastChangedBy:: rgruendl               $:  # Author of last commit.
# $LastCha

"""
From a given proctag create/append a new sub-proctag based on a list of
exposure numbers.

Must have either -e (expnum) or -l file containing a list of eposures 
numbers (NOTE both can be used).  The list file has the following format.
     expnum, {any desired extraneous information/comments) 
Lines beginning with "#" are ignored.  

Note: Currently the OPS_PROCTAG_DEF must be created manually.

Optional outputs:
  --yearly  
  --updateDB causes the database update to occur as part of the program execution.

Arguments:
     
"""
##########################################
def  check_tag_exists(tag,dbh,dbSchema,verbose=0):
    """Function that simply checks that an entry exists for a specific tag"""

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

    if (len(queryResult)>0):
        TagExists=True
        if (len(queryResult)>1):
            print("# WARNING: Found more that one result (#={:d}) for tag={:s} in {:s}ops_proctag_def".format(len(queryResult),tag,dbSchema))
    else:
        TagExists=False
        print("# WARNING: tag={:s} does not exist in {:s}ops_proctag_def".format(tag,dbSchema))
#
#       Give the user a chance to create a new entry in OPS_PROCTAG_DEF
#
        # Check if we want to add TAG
        answer = 'yes'
        answer = input('Would you like to add TAG={:s} to OPS_PROCTAG_DEF?\n[YES/no]: '.format(tag))
#        print answer
        if ('YES' in answer.upper()) or ('Y' in answer.upper()):

            cur = dbh.cursor()
            description = input('Please Enter Short TAG Description:\n')
            docurl      = input('Please Enter DOCURL [Optional]:\n')
            I_TAG = "insert into OPS_PROCTAG_DEF (TAG,DESCRIPTION,DOCURL) values ('{tag}','{description}','{docurl}')"
            print("# Inserting {:s} into OPS_PROCTAG_DEF".format(tag))
            cur.execute(I_TAG.format(tag=tag,description=description,docurl=docurl))
            cur.close()
            dbh.commit()
            TagExists=True

    return TagExists


##########################################
def find_exp_attempts(ExpnumList,ProcTag,dbh,dbSchema,verbose=0):
    """Function that searchs for a set of attempts that processed a list of exposures"""

    ExpList=[]
    for expnum in ExpnumList:
        ExpList.append([expnum])
#
#   Make sure the GTT_EXPNUM table is empty
    curDB=dbh.cursor()
    curDB.execute('delete from GTT_EXPNUM')
#    # load img ids into opm_filename_gtt table
    print("# Loading GTT_EXPNUM for secondary queries with entries for {:d} images".format(len(ExpList)))
    dbh.insert_many('GTT_EXPNUM',['EXPNUM'],ExpList)
#    dbh.commit()
#    curDB.execute('select count(*) from gtt_expnum')
#    for row in curDB:
#        print row 

#
#   Now the main query
#
    query="""SELECT 
        el.expnum as expnum,
        a.id as pfw_attempt_id,
        a.reqnum  as reqnum,
        a.unitname as unitname,
        a.attnum  as attnum
    FROM {schema:s}pfw_attempt a, {schema:s}pfw_attempt_val av, {schema:s}proctag t, gtt_expnum el
    WHERE t.tag='{ptag:s}'
        and t.pfw_attempt_id=av.pfw_attempt_id
        and av.key='expnum'
        and av.val=to_char(el.expnum,'999999')
        and t.pfw_attempt_id=a.id
    """.format(schema=dbSchema,ptag=ProcTag)

    if (verbose > 0):
        print("# Executing query to obtain existing attempts for set of expsures")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    queryResult={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        expnum=int(rowd['expnum'])
        if (expnum in queryResult):
            print("Warning: multiple entries detected in tag={:s} for expnum={:d}.".format(ProcTag,expnum))
#        print expnum,rowd
        queryResult[expnum]=rowd
    t1=time.time()

    return queryResult


##########################################
def find_tile_attempts(TileList,ProcTag,dbh,dbSchema,verbose=0):
    """Function that searchs for a set of attempts that processed a list of exposures"""

    tmpTileList=[]
    for tile in TileList:
        tmpTileList.append([tile])
#
#   Make sure the GTT_STR table is empty
    curDB=dbh.cursor()
    curDB.execute('delete from GTT_STR')
#    # load img ids into opm_filename_gtt table
    print("# Loading GTT_STR for secondary queries with entries for {:d} tiles".format(len(tmpTileList)))
    dbh.insert_many('GTT_STR',['STR'],tmpTileList)
#    dbh.commit()
#    curDB.execute('select count(*) from gtt_expnum')
#    for row in curDB:
#        print row 

#
#   Now the main query
#
    query="""SELECT 
        av.val as tilename,
        a.id as pfw_attempt_id,
        a.reqnum  as reqnum,
        a.unitname as unitname,
        a.attnum  as attnum
    FROM {schema:s}pfw_attempt a, {schema:s}pfw_attempt_val av, {schema:s}proctag t, gtt_str gs
    WHERE t.tag='{ptag:s}'
        and t.pfw_attempt_id=av.pfw_attempt_id
        and av.key='tilename'
        and av.val=gs.str
        and t.pfw_attempt_id=a.id
    """.format(schema=dbSchema,ptag=ProcTag)

    if (verbose > 0):
        print("# Executing query to obtain existing attempts for set of tiles")
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    queryResult={}
    for row in curDB:
        rowd = dict(zip(desc, row))
        tilename=rowd['tilename']
        if (tilename in queryResult):
            print("Warning: multiple entries detected in tag={:s} for tilename={:s}.".format(ProcTag,tilename))
#        print expnum,rowd
        queryResult[tilename]=rowd
    t1=time.time()

    return queryResult


##########################################
def find_current_tag(TagName,TagType,dbh,dbSchema,verbose=0):
    """Function that simply checks what entries currently exist for a specific tag"""
#
#   Form the main query
#
    if (tagtype == "EXP"):
        query="""SELECT 
            to_number(av.val,'999999') as expnum,
            av.pfw_attempt_id as pfw_attempt_id
        FROM {schema:s}pfw_attempt_val av, {schema:s}proctag t
        WHERE t.tag='{ptag:s}'
            and t.pfw_attempt_id=av.pfw_attempt_id
            and av.key='expnum'
        """.format(schema=dbSchema,ptag=TagName)
    else:
        query="""SELECT 
            av.val as tilename,
            av.pfw_attempt_id as pfw_attempt_id
        FROM {schema:s}pfw_attempt_val av, {schema:s}proctag t
        WHERE t.tag='{ptag:s}'
            and t.pfw_attempt_id=av.pfw_attempt_id
            and av.key='tilename'
        """.format(schema=dbSchema,ptag=TagName)

    if (verbose > 0):
        print("# Executing query to check for existing entries in TAG {:s}".format(TagName))
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
        if (tagtype == "EXP"):
            expnum=int(rowd['expnum'])
            if (expnum in queryResult):
                print("Warning: multiple entries detected in tag={:s} for expnum={:d}.".format(TagName,expnum))
            queryResult[expnum]=rowd
        else:
            tilename=rowd['tilename']
            if (tilename in queryResult):
                print("Warning: multiple entries detected in tag={:s} for tilename={:s}.".format(TagName,tilename))
            queryResult[tilename]=rowd
    t1=time.time()

    return queryResult


#################################
def insert_tag_entries(AttemptDict,ExistingTagDict,DBTable,DBorder,DtoC,dbh,updateDB,verbose=0):
    """Ingest new tag entries"""

#
#   Code is meant to work for both exposure based and tilename based tags.
#   Automatically detects the difference by checking for the dictionary key 'expnum'
#

    t0=time.time()
    InsertCnt=0
    print("Preparing list of lists to ingest entries")

#
#   Preliminary sanity check (Do I know which data comes from where)
#
    CheckCheckIt=True
    for col in DBorder:
        if (col not in DtoC):
            print('No entry for column ({:s}) in DtoC.  Unable to Proceed!'.format(col))
            CheckCheckIt=False
    if (not(CheckCheckIt)):
        print('Aborting!')
        exit(1)

    new_data=[]
    for key in AttemptDict:
#        add_to_insert=True
        if (key in ExistingTagDict):
            if ('expnum' in ExistingTagDict[key]):
                print("Entry for expnum={:d} already exists.  Skipping entry.".format(key))
            else:
                print("Entry for tilename={:s} already exists.  Skipping entry.".format(key))
            if (AttemptDict[key]['pfw_attempt_id'] != ExistingTagDict[key]['pfw_attempt_id']):
                print("Need to add ability to update?")
        else:
            new_row=[]
            for col in DBorder:
                if (DtoC[col]['src'] == 'dict'):
                    new_row.append(AttemptDict[key][DtoC[col]['col']])
                elif (DtoC[col]['src'] == 'arg'):
                    new_row.append(DtoC[col]['val'])
                else:
                    new_row.append(None)
            new_data.append(new_row)

    t1=time.time()
    print("Successfully Formed list for Ingestion (Nrows={:d}, Timing: {:.2f})".format(len(new_data),(t1-t0)))

    if (updateDB):
        print("# Loading {:s} with {:d} entries".format(DBTable,len(new_data)))
        t1=time.time()
        dbh.insert_many(DBTable,DBorder,new_data)
        t2=time.time()
        print("Commit of {:d} rows, timing={:.2f} of {:.2f})".format(len(new_data),(t2-t1),(t2-t0)))
        dbh.commit()
    else:
        print("# Dry run... No update requested (i.e. --updateDB was not set).")
    ningest=len(new_data)

    return ningest


##########################################
if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
    import stat
    import time
    import re
    import sys
    from datetime import datetime

    DBTable="proctag"
    svnid="$Id: make_sub_proctag.py 43088 2016-07-12 18:03:45Z rgruendl $"

    parser = argparse.ArgumentParser(description='Create tag for relavent exposure numbers')
    parser.add_argument('--proctag',        action='store', type=str, required=True, help='Existing proctag from which existing PFW_ATTEMPT_IDs are drawn')
    parser.add_argument('--tag',            action='store', type=str, required=True, help='SubTag where PFW_ATTEMPT_IDs additions are made')
    parser.add_argument('--expnum',         action='store', type=str, default=None,  help='Exposure number (or comma delimited list of exposure numbers)')
    parser.add_argument('--tilename',       action='store', type=str, default=None,  help='TileName (or comma delimited list of TileNames)')
    parser.add_argument('-l', '--list',     action='store', type=str, default=None,  help='File containing a list of exposure numbers')
    parser.add_argument('-u', '--updateDB', action='store_true',      default=False, help='Flag for program to DIRECTLY update DB (proctag).')
    parser.add_argument('-D', '--DB_file',  action='store', type=str, default=None,  help='Optional output of DB update file')
#    parser.add_argument('-C', '--Comment',  action='store', type=str, default=None, help='Optional comment/explanation')
#    parser.add_argument('--RUA',            action='store_true',      default=False, help='Specify ReqNum,Unitname,AttNum when updating table')
    parser.add_argument('--tagtype',        action='store', type=str, default='EXP', help='Tag Type is either single-epoch (exp) or multi-epoch (tile)')
    parser.add_argument('--over_table',     action='store', type=str, default=None,  help='Override output DB table with specified table')
    parser.add_argument('--analyst',        action='store', type=str, default=None,  help='Optional override showing who made the entries (default is to obtain form os.getlogin())')
    parser.add_argument('-s', '--section',  action='store', type=str, default=None,  help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',   action='store', type=str, default=None,  help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose',  action='store', type=int, default=0,     help='Print extra (debug) messages to stdout')
    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)

    verbose=args.verbose

    tagtype=args.tagtype.upper()
    if (tagtype == "EXP"):
        if (verbose > 0):
            print("Working on single-epoch proctag (i.e. exposure-based)")
    elif (tagtype == "TILE"):
        if (verbose > 0):
            print("Working on multi-epoch proctag (i.e. tile-based)")
        print("Currently tagtype == TILE is not supported!!!!")
#        exit(0)
    else:
        print("Invalid tag-type ({:s}).  Must be either EXP or TILE".format(tagtype))
        print("Aborting!")
        exit(1)

#
#   Logic will need updating when multi-epoch tags are added.
#
    if (tagtype == "EXP"):
        if ((args.expnum is None)and(args.list is None)):
            print(" ")
            print("ERROR: Must provide one (or both) of the following.")
            print(" 1) an expnum or list of expnum (-e), or ")
            print(" 2) a file containg a list of exposure numbers (-l).")
            print("Aborting!")
            print(" ")
            parser.print_help()
            exit(1)
    elif (tagtype == "TILE"):
        if ((args.tilename is None)and(args.list is None)):
            print(" ")
            print("ERROR: Must provide one (or both) of the following.")
            print(" 1) an tilename or list of tilenames (--tilename), or ")
            print(" 2) a file containg a list of tilenames (-l).")
            print("Aborting!")
            print(" ")
            parser.print_help()
            exit(1)

#
#   Automatically get analyst name (from os.getlogin()) unless an override value has been specified
#
    if ((args.analyst is None)or(args.analyst == "None")):
        analyst=os.getlogin()
    else:
        analyst=args.analyst

#
#   Check for user define DB schema and whether DB table is being over ridden
#
    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)

    if (args.over_table is None):
        DBTable='%s%s' % (dbSchema,DBTable)
    else:
        if (len(args.over_table.split('.')) > 1):
            DBTable=args.over_table
        else:
            DBTable='%s%s' % (dbSchema,args.over_table)

#
#   If an optional DB_file (containing INSERT commands) is chosen the set flag...
#
    if (args.DB_file is None):
        use_DB_file=False
    else:
        use_DB_file=True

#
#   populate the expnum_list/tilelist with values
#
    if (tagtype == "EXP"):
        ExpNumList=[]
        if (not(args.expnum is None)):
            tmp_list=args.expnum.split(',')
            for tmp_entry in tmp_list:
                if (tmp_entry.strip() != ''):
                    expnum=int(tmp_entry.strip())
                    ExpNumList.append(expnum)

        if (not(args.list is None)):
            if os.path.isfile(args.list):
                f1=open(args.list,'r')
                for line in f1:
                    line=line.strip()
                    columns=line.split(' ')
                    if (columns[0] != "#"):
                        tmp_exprec={}
                        expnum=int(columns[0])
                        ExpNumList.append(expnum)
                f1.close()
            else:
                print("Warning: File {:s} not found!".format(args.list))
                print("Aborting!")
                exit(1)
        print("Formed exposure list for processing: {:d} exposures found.".format(len(ExpNumList)))
    else:
        TileList=[]
        if (not(args.tilename is None)):
            tmp_list=args.tilename.split(',')
            for tmp_entry in tmp_list:
                if (tmp_entry.strip() != ''):
                    tile=tmp_entry.strip()
                    TileList.append(tile)

        if (not(args.list is None)):
            if os.path.isfile(args.list):
                f1=open(args.list,'r')
                for line in f1:
                    line=line.strip()
                    columns=line.split(' ')
                    if (columns[0] != "#"):
                        tmp_exprec={}
                        tile=columns[0]
                        TileList.append(tile)
                f1.close()
            else:
                print("Warning: File {:s} not found!".format(args.list))
                print("Aborting!")
                exit(1)
        print("Formed tile list for processing: {:d} tilenames found.".format(len(TileList)))

#
#   Setup DB connection.
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
#    cur = dbh.cursor()

################################################################################################
################################################################################################
#  
#   Quick check to make sure that a tag exists (if not... then all is for naught)
#

    if (check_tag_exists(args.tag,dbh,dbSchema,verbose)):
        if (verbose>0):
            print("Check for existing tag: '{:s}' successfull.".format(args.tag))
    else:
        print("Aborting!")
        exit(1)

#   Find existing processing attempts for the list of exposures.
    t0=time.time()
    if (tagtype == "EXP"):
        AttemptDict = find_exp_attempts(ExpNumList,args.proctag,dbh,dbSchema,verbose)
    else:
        AttemptDict = find_tile_attempts(TileList,args.proctag,dbh,dbSchema,verbose)
    t1=time.time()
    if (verbose > 0):
        print("Query to find attempts identified {:d} entries.  Execution time was: {:.2f}".format(len(AttemptDict),(t1-t0)))

#
#   Check for missing entries
#
    if (tagtype == "EXP"):
        found_exp=[]
        for expnum in ExpNumList:
            if (expnum in AttemptDict):
                found_exp.append(expnum)
            else:
                if (verbose > 0):
                    print("No entry found for expnum={:d}".format(expnum))
    else:
        found_tile=[]
        for tile in TileList:
            if (tile in AttemptDict):
                found_tile.append(tile)
            else:
                if (verbose > 0):
                    print("No entry found for tilename={:s}".format(tile))
#
#   Check for existing entries
#
    t0=time.time()
    ExistingTagDict = find_current_tag(args.tag,tagtype,dbh,dbSchema,verbose)
    t1=time.time()
    if (verbose > 0):
        print("Query to probe the tag ({:s}) identified {:d} existing entries.  Execution time was: {:.2f}".format(args.tag,len(ExistingTagDict),(t1-t0)))

#
#   Ingest Results
#
#   Establish a common time for database inserts.
    common_time=datetime.now()
    DBorder=['REQNUM','UNITNAME','ATTNUM','TAG','CREATED_DATE','CREATED_BY','PFW_ATTEMPT_ID']

    DBtoCSV_ColDict={
        'REQNUM':         {'src':'dict','col':'reqnum'},
        'UNITNAME':       {'src':'dict','col':'unitname'},
        'ATTNUM':         {'src':'dict','col':'attnum'},
        'TAG':            {'src':'arg', 'val':args.tag},
        'CREATED_DATE':   {'src':'arg', 'val':common_time},
        'CREATED_BY':     {'src':'arg', 'val':analyst},
        'PFW_ATTEMPT_ID': {'src':'dict','col':'pfw_attempt_id'}
    }

    t0=time.time()
    num_insert=insert_tag_entries(AttemptDict,ExistingTagDict,DBTable,DBorder,DBtoCSV_ColDict,dbh,args.updateDB,verbose)
    t1=time.time()
    if (verbose > 0):
        print("Finished DB inserts of {:d} entries for tag={:s}.  Execution time was: {:.2f}".format(num_insert,args.tag,(t1-t0)))

    exit(0)    
