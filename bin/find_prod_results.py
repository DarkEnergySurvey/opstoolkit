#! /usr/bin/env python
"""
Query a night (or range of nights) to determine the SN sequences present
and output a list suitable for mass-submits.

Currently sequences are always reset to 1 (not elegant as it relies on --exclude
"""

if __name__ == "__main__":

    import argparse
    import os
    from despydb import DesDbi 
    from opstoolkit import jiracmd
    from opstoolkit import nite_strings
    import stat
    import time
    import csv
    import sys
    import datetime
    import numpy

    parser = argparse.ArgumentParser(description='Produce listing(s) of supernova sets (for submit) from a night range')
    parser.add_argument('--first',   action='store', type=str, default=None, help='First night (nite) to consider')
    parser.add_argument('--last',    action='store', type=str, default=None, help='Last night (nite) to consider')
    parser.add_argument('--night',   action='store', type=str, default=None, help='Night (nite) to consider')
    parser.add_argument('--fileout', action='store', type=str, default=None, help='Summary listing filename (default is STDOUT)')
    parser.add_argument('--jira',    action='store', type=str, default=None, help='Confine query to a ticket or sub-tickets under a JIRA ticket')
    parser.add_argument('--only_parent', action='store_true', default=False, help='Flag to only search under the indicated ticket')
    parser.add_argument('--only_failed', action='store_true', default=False, help='Flag to only report on submissions that have resulted in failure')
    parser.add_argument('--only_running', action='store_true', default=False, help='Flag to only report on submissions that are currently running')
    parser.add_argument('--junk',    action='store_true', default=False, help='Flag to suppress junk runs')
    parser.add_argument('--terse',   action='store_true', default=False, help='Flag to give only terse summary for each run')
    parser.add_argument('--full',    action='store_true', default=False, help='Flag to processing summary for each module run')
    parser.add_argument('--fid_date', action='store', type=str, default=None, help='Define a fiducial date/time to compare submit times (format=YYYYMMDD:HHMMSS, default=None)')
#    parser.add_argument('-q', '--quiet',   action='store_true', default=False, help='Flag to run as silently as possible')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section', action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

    if ((args.night is None)and(args.first is None)):
        print "Must specify --night or --first for query"
        exit(1)
    if (args.jira is None):
        print "Must specify --jira {parent} for query"
        exit(1)

    if (args.night is not None):
        f_night=args.night
        l_night=args.night
    else:
        f_night=args.first
        if (args.last is None):
            l_night=args.first
        else:
            l_night=args.last

    nite_list=[]
    nite_list.append(f_night)
    while (nite_list[-1] < l_night):
       nite_list.append(nite_strings.increment_nite(nite_list[-1]))
    if (nite_list[-1]>l_night):
       nite_list=nite_list[:-1]

    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

    if (args.junk):
        suppress_junk=" and a.data_state!='JUNK'"
    else:
        suppress_junk=""

    if (args.only_running):
        only_running=" and t.status is null "
    else:
        only_running=""

    if (args.only_failed):
        only_failed=" and t.status != 0 "
    else:
        only_failed=""

#
#   If an association with a JIRA ticket has been requested then make sure that a connection can be made to jira also.
#   NOTE: Could use try for inital connect if suitable means to catch an error was known....
#
    jira_connect = jiracmd.Jira('jira-desdm')
    try:
        check_parent_issue = jira_connect.jira.issue(args.jira)
    except:
        print 'Parent issue %s does not exist!' % args.jira
        sys.exit()

    reqnum_dict={}
    if (args.only_parent):
        reqnum = str(args.jira.split('-')[1])
        try:
            nite=int(check_parent_issue.fields.summary)
        except:
#           Try parsing through the text to see if there is something resembling a date
            for col in check_parent_issue.fields.summary.split():
                try:
                    use_nite=int(col)
                    if ((use_nite > 20120901)and(use_nite < 20200501)):
                        nite=use_nite
                        break
                except:
                    pass
            else:
                nite=-1
        if ((nite > 20120901)and(nite < 20200501)):
            reqnum_dict[nite]=reqnum
        else:
#           Ideally the "-1" here needs to be replaced by a nite.
#           However search of the JIRA ticket summary for a night is the only way this is attempted now.
            reqnum_dict[-1]=reqnum
    else:
        for nite in nite_list:
            subissue_exists = jira_connect.search_for_issue(args.jira,nite)
            if (subissue_exists[1] != 0):
                if (args.verbose):
                    print "#JIRA ticket exists for %s. Will use %s." % (nite,subissue_exists[0][0].key)
                reqnum_dict[nite]=str(subissue_exists[0][0].key).split('-')[1]

#
#   Setup DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = DesDbi(desdmfile,args.section)
    cur = dbh.cursor()

#
#   Form a list of request numbers for the nights under the parent ticket (or only the parent ticket itself
#
    attempt_list=[]
    req_list=[]
    for key in reqnum_dict:
        req_list.append(reqnum_dict[key])
    req_list_constraint="a.reqnum in (%s)" % (",".join(req_list))
#
#   Main (workhorse) query to collect requests under a specific ticket
#
    queryitems = ["a.reqnum", "a.unitname", "a.attnum","a.submittime","a.data_state","a.archive_path","a.task_id","t.start_time","t.end_time","t.status"]
    querylist = ",".join(queryitems)
    coldict={}
    for index, item in enumerate(queryitems):
        coldict[item]=index
    query = """select %s from %spfw_attempt a, %stask t where %s %s %s %s and a.task_id=t.id order by a.submittime """ % ( querylist, db_Schema, db_Schema, req_list_constraint,suppress_junk,only_running,only_failed)

    if args.verbose:
        print query
    cur.arraysize = 1000 # get 1000 at a time when fetching
    cur.execute(query)

    tfirst_submit=None;
    for item in cur:
        tmp_dict={}
        for nite in reqnum_dict:
            if (int(item[coldict["a.reqnum"]]) == int(reqnum_dict[nite])):
                tmp_dict["nite"]=int(nite)
        if ("nite" not in tmp_dict):
            tmp_dict["nite"]=-1
        tmp_dict["reqnum"]=int(item[coldict["a.reqnum"]])
        tmp_dict["unitname"]=item[coldict["a.unitname"]]
        tmp_dict["attnum"]=int(item[coldict["a.attnum"]])
        tmp_dict["stime"]=item[coldict["a.submittime"]]
        if (len(attempt_list)==0):
            if (args.fid_date is not None):
                tfirst_submit=datetime.datetime.strptime(args.fid_date,"%Y%m%d:%H%M%S")
            else:
                tfirst_submit=item[coldict["a.submittime"]]
        tmp_dict["state"]=item[coldict["a.data_state"]]
        tmp_dict["path"]=item[coldict["a.archive_path"]]
        if (item[coldict["t.status"]] is None):
            tmp_dict["status"]=-1
            tmp_dict["end_time"]="processing"
            tmp_dict["proctime"]=-1
            tmp_dict["totalwall"]=-1
            tmp_dict["tdays"]=-1
        else:
            tmp_dict["status"]=int(item[coldict["t.status"]])
            if (item[coldict["t.start_time"]] is None):
                tmp_dict["start_time"]=-1
            else:            
                tmp_dict["start_time"]=item[coldict["t.start_time"]].strftime("%Y%m%d:%H%M%S")
            if (item[coldict["t.end_time"]] is None):
                tmp_dict["end_time"]="processing"
            else:            
                tmp_dict["end_time"]=item[coldict["t.end_time"]].strftime("%Y%m%d:%H%M%S")
            if ((item[coldict["t.end_time"]] is None)or(item[coldict["t.start_time"]] is None)):
                tmp_dict["proctime"]=-1.
            else:
                tmp_dict["proctime"]=(item[coldict["t.end_time"]]-item[coldict["t.start_time"]]).total_seconds()
            if ((item[coldict["t.end_time"]] is None)or(item[coldict["a.submittime"]] is None)):
                tmp_dict["totalwall"]=-1.
            else:
                tmp_dict["totalwall"]=(item[coldict["t.end_time"]]-item[coldict["a.submittime"]]).total_seconds()
            if ((tfirst_submit is None)or(item[coldict["t.end_time"]] is None)):
                tmp_dict["tdays"]=-1.
            else:
                tmp_dict["tdays"]=(item[coldict["t.end_time"]]-tfirst_submit).total_seconds()/(3600.*24.)
        attempt_list.append(tmp_dict)

    print("{:8s} {:15s} {:6s} {:2s} {:15s} {:15s} {:7s} {:7s} {:8s} {:6s} {:s} ".format("#"," "," "," "," "," ","  Proc","Elapsed"," "," "," "))
    print("{:8s} {:15s} {:6s} {:2s} {:15s} {:15s} {:7s} {:7s} {:8s} {:6s} {:s} ".format("# nite","fld-band-seq","reqnum","a#","  Submit Time","    End Time","  [s]","  [d]","  State","Status","  Filepath"))
    for attempt in attempt_list:
        print("{:8d} {:15s} {:6d} {:2d} {:15s} {:15s} {:7d} {:7.3f} {:8s} {:6d} {:s} ".format(attempt["nite"],attempt["unitname"],attempt["reqnum"],attempt["attnum"],attempt["stime"].strftime("%Y%m%d:%H%M%S"),attempt["end_time"],int(attempt["proctime"]),attempt["tdays"],attempt["state"][:7],attempt["status"],attempt["path"]))
    print("# End summary ")

#
#   Done at this point if "--terse" was chosen.  Otherwise continue and find out specific info about each run.
#


    if (not(args.terse)):
        print("#")
        print("#")
#
#       Form a dictionary to hold the details of the processing
##
        proc_dict={}
        for attempt in attempt_list:
            proc_attempt="%d:%s:%d" % (attempt['reqnum'],attempt['unitname'],attempt['attnum'])
            print("#############################################################")
            print("# {:s}".format(proc_attempt))
            proc_dict[proc_attempt]={}
#
#           Get list of modules that were/are to be executed.
#
            queryitems = ["b.blknum", "lower(b.modulelist)"]
            querylist = ",".join(queryitems)
            coldict={}
            for index, item in enumerate(queryitems):
                coldict[item]=index
            query = """select %s from %spfw_block b where b.reqnum=%d and b.unitname='%s' and b.attnum=%d order by b.blknum """ % ( querylist, db_Schema, attempt['reqnum'],attempt['unitname'],attempt['attnum'])

            if args.verbose:
                print query
            cur.arraysize = 1000 # get 1000 at a time when fetching
            cur.execute(query)

            mod_list=[]
            for item in cur:
                for mod in item[coldict["lower(b.modulelist)"]].split(','):
                    proc_dict[proc_attempt][mod]={}
                    proc_dict[proc_attempt][mod]['fail']=0
                    proc_dict[proc_attempt][mod]['pass']=0
                    proc_dict[proc_attempt][mod]['proc']=0
                    proc_dict[proc_attempt][mod]['fail_jk']=[]
                    proc_dict[proc_attempt][mod]['proc_jk']=[]
                    mod_list.append(mod)
            proc_dict[proc_attempt]['modlist']=mod_list

#
#           Now look at specific of what has been run (and check for failures)
#
            queryitems = ["w.modname","e.task_id","e.name","e.status","j.jobkeys"]
            querylist = ",".join(queryitems)
            coldict={}
            for index, item in enumerate(queryitems):
                coldict[item]=index
            query = """select %s from %spfw_wrapper w, %spfw_exec e, %spfw_job j where w.reqnum=%d and w.unitname='%s' and w.attnum=%d and w.reqnum=e.reqnum and w.unitname=e.unitname and w.attnum=e.attnum and e.wrapnum=w.wrapnum and w.reqnum=j.reqnum and w.unitname=j.unitname and w.attnum=j.attnum and w.jobnum=j.jobnum """ % ( querylist, db_Schema, db_Schema, db_Schema, attempt['reqnum'],attempt['unitname'],attempt['attnum'])

            if args.verbose:
                print query
            cur.arraysize = 1000 # get 1000 at a time when fetching
            cur.execute(query)

            for item in cur:
                mname=item[coldict["w.modname"]].lower()
                if (mname not in proc_dict[proc_attempt]):
                    print "# Warning: Missing module ",mname," (added)"
                    proc_dict[proc_attempt][mname]={}
                    proc_dict[proc_attempt][mname]['fail']=0
                    proc_dict[proc_attempt][mname]['pass']=0
                    proc_dict[proc_attempt][mname]['proc']=0
                    proc_dict[proc_attempt][mname]['fail_jk']=[]
                    proc_dict[proc_attempt][mname]['proc_jk']=[]
                    proc_dict[proc_attempt]['modlist'].append(mname)
                if (item[coldict["e.status"]] is not None):
                    if (int(item[coldict["e.status"]])!=0):
                        proc_dict[proc_attempt][mname]['fail']=proc_dict[proc_attempt][mname]['fail']+1
                        proc_dict[proc_attempt][mname]['fail_jk'].append(item[coldict["j.jobkeys"]])
                    else:
                        proc_dict[proc_attempt][mname]['pass']=proc_dict[proc_attempt][mname]['pass']+1
                else:
                    proc_dict[proc_attempt][mname]['proc']=proc_dict[proc_attempt][mname]['proc']+1
                    proc_dict[proc_attempt][mname]['proc_jk'].append(item[coldict["j.jobkeys"]])
#                    print item[coldict["j.jobkeys"]]
            mod_list=[]
            mod_unfinished=0
            for mod in proc_dict[proc_attempt]['modlist']:
                if ((proc_dict[proc_attempt][mod]['fail']>0)or(proc_dict[proc_attempt][mod]['proc']>0)or(args.full)):
                    mod_list.append(mod)
                if ((proc_dict[proc_attempt][mod]['fail']>0)or(proc_dict[proc_attempt][mod]['proc']>0)):
                    mod_unfinished=mod_unfinished+1
            if (mod_unfinished==0):
                print("#  No failures or running jobs detected")
            for mod in mod_list:
                fail_sum_jk=""
                proc_sum_jk=""
                if (proc_dict[proc_attempt][mod]['fail']>0):
                    fail_sum_jk="Failed: " + ",".join(proc_dict[proc_attempt][mod]['fail_jk'])
                if (proc_dict[proc_attempt][mod]['proc']>0):
                    proc_sum_jk="Running: " + ",".join(proc_dict[proc_attempt][mod]['proc_jk'])
                print("   {:5d} {:5d} {:5d} {:30s} {:s} {:s} ".format(proc_dict[proc_attempt][mod]['pass'],proc_dict[proc_attempt][mod]['fail'],proc_dict[proc_attempt][mod]['proc'],mod,fail_sum_jk,proc_sum_jk))

    exit(0)
