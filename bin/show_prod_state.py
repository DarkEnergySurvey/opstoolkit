#! /usr/bin/env python3
"""
show_prod_state.py 
Original:  RAG w/python3 migration Sept 4, 2020.

Polls DESDM (Operations) DB for a given pipeline execution and returns summary of status.
"""

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
#    from opstoolkit import jiracmd
    from opstoolkit import nite_strings
    import stat
    import time
    import csv
    import sys
    import datetime
    import numpy

    parser = argparse.ArgumentParser(description='Produce summary of pipeline execution')
    parser.add_argument('-A','--AttemptID',action='store', type=str, default=None, help='PFW Attempt ID')
    parser.add_argument('-r','--ReqNum',   action='store', type=str, default=None, help='Request number')
    parser.add_argument('-u','--UnitName', action='store', type=str, default=None, help='Unit name')
    parser.add_argument('-a','--AttNum',   action='store', type=str, default=None, help='Attempt number')
    parser.add_argument('--terse',   action='store_true', default=False, help='Flag to give only terse summary for run')
    parser.add_argument('--full',    action='store_true', default=False, help='Flag to processing summary for each module run')
    parser.add_argument('--detail',  action='store',      type=str,  default=None, help='Optional detailed runtime output')
    parser.add_argument('-v', '--verbose', action='store', type=int, default=0,    help='Print extra (debug) messages to stdout (default=0, higher values are more verbose)')
    parser.add_argument('-s', '--section', action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose > 0):
        print("Args: ",args)

    verbose=args.verbose
#    if (args.verbose):
#        verbose=1

    if (args.AttemptID is None):
        if ((args.ReqNum is None)or(args.UnitName is None)or(args.AttNum is None)):
            print("Must specify either AttemptID or triplet (ReqNum, UnitName, and AttNum) for query")
            exit(1)

    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

#
#   Setup DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
    cur = dbh.cursor()

    if (args.AttemptID is None):
        query = """SELECT a.id as pfw_attempt_id
        FROM {schema:s}pfw_attempt a 
        WHERE a.reqnum={rnum:s}
            and a.unitname='{uname:s}' 
            and a.attnum={anum:s}
        """.format(schema=db_Schema,rnum=args.ReqNum,uname=args.UnitName,anum=args.AttNum)

        if (verbose > 0):
            if (verbose == 1):
                QueryLines=query.split('\n')
                QueryOneLine='sql = '
                for line in QueryLines:
                    QueryOneLine=QueryOneLine+" "+line.strip()
                print(QueryOneLine)
            if (verbose > 1):
                print(query)

        cur.execute(query)
        desc=[d[0].lower() for d in cur.description]
        for row in cur:
            rowd=dict(zip(desc,row))
            AttemptID=rowd['pfw_attempt_id']

    else:
        AttemptID=int(args.AttemptID)


#
#   Main (workhorse) query to collect requests under a specific ticket
#
#    queryitems = ["a.reqnum", "a.unitname", "a.attnum","a.submittime","a.data_state","a.archive_path","a.task_id","t.start_time","t.end_time","t.status"]
#    querylist = ",".join(queryitems)
#    coldict={}
#    for index, item in enumerate(queryitems):
#        coldict[item]=index
    query = """SELECT
        a.reqnum as reqnum, a.unitname as unitname, a.attnum as attnum,
        a.submittime as stime,
        (cast(SYSDATE as date)-cast(a.submittime as date)) as tdays,
        a.data_state as state,
        a.archive_path as path,
        a.task_id as task_id,
        (cast(t.end_time as date)-cast(t.start_time as date)) as walltime,
        (cast(SYSDATE as date)-cast(t.start_time as date)) as proctime,
        t.status as status
    FROM {schema:s}pfw_attempt a, {schema:s}task t 
    WHERE a.id={AttID:d} 
        and a.task_id=t.id""".format(schema=db_Schema,AttID=AttemptID)

    if (verbose > 0):
        if (verbose == 1):
            QueryLines=query.split('\n')
            QueryOneLine='sql = '
            for line in QueryLines:
                QueryOneLine=QueryOneLine+" "+line.strip()
            print(QueryOneLine)
        if (verbose > 1):
            print(query)
    cur.execute(query)
    desc=[d[0].lower() for d in cur.description]

    attempt_list=[]
    tfirst_submit=None;
    for row in cur:
        rowd=dict(zip(desc,row))
#        print(rowd)
        if (rowd['status'] is None):
            rowd['status']=-1
            rowd['endstate']='processing'
        else:
            rowd['endstate']='finished'
        if (rowd['walltime'] is None):
            rowd['walltime']=-1
#        rowd['start_time']=rowd['start_time'].strftime("%Y%m%d:%H%M%S")
#        if (rowd['end_time'] is None):
#            rowd['end_time']="processing"
#        else:
#            rowd['end_time']=rowd['end_time'].strftime("%Y%m%d:%H%M%S")
#            if ('proctime' not in rowd):
#                rowd['proctime']=(item[coldict["t.end_time"]]-item[coldict["t.start_time"]]).total_seconds()
#            if ((item[coldict["t.end_time"]] is None)or(item[coldict["a.submittime"]] is None)):
#                tmp_dict["totalwall"]=-1.
#            else:
#                tmp_dict["totalwall"]=(item[coldict["t.end_time"]]-item[coldict["a.submittime"]]).total_seconds()
#            tmp_dict["tdays"]=-1.
        attempt_list.append(rowd)

#    print(attempt_list)

    print("{:15s} {:6s} {:2s} {:15s} {:15s} {:7s} {:7s} {:8s} {:6s} {:s} ".format('#',' ',' ',' ',' ','  Proc','Elapsed',' ',' ',' '))
    print("{:15s} {:6s} {:2s} {:15s} {:15s} {:7s} {:7s} {:8s} {:6s} {:s} ".format('# fld-band-seq','reqnum','a#','  Submit Time','    End Time','  [s]','  [d]','  State','Status','  Filepath'))
    print("{:15s} {:6s} {:2s} {:15s} {:15s} {:7s} {:7s} {:8s} {:6s} {:s} ".format('# ------------','------','--','-------------','------------','-----','-----','-------','------','----------'))
    for attempt in attempt_list:
        print("{Uname:15s} {Rnum:6d} {Anum:2d} {Stime:15s} {Estate:15s} {Wtime:7d} {Tdays:7.3f} {State:8s} {Stat:6d} {Path:s} ".format(
            Uname=attempt["unitname"],
            Rnum=attempt["reqnum"],
            Anum=attempt["attnum"],
            Stime=attempt["stime"].strftime("%Y%m%d:%H%M%S"),
            Estate=attempt['endstate'],
            Wtime=int(attempt["walltime"]),
            Tdays=attempt["tdays"],
            State=attempt["state"][:7],
            Stat=attempt["status"],
            Path=attempt["path"]))
    print("# End summary ")
#            Stime='blah',

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
#            queryitems = ["b.blknum", "lower(b.modulelist)"]
#            querylist = ",".join(queryitems)
#            coldict={}
#            for index, item in enumerate(queryitems):
#                coldict[item]=index
            query = """SELECT b.blknum as blknum,
                lower(b.modulelist) as modulelist
            FROM {schema:s}pfw_block b 
            WHERE b.pfw_attempt_id={AttID:d} 
            ORDER BY b.blknum """.format(schema=db_Schema, AttID=AttemptID)

            if (verbose > 0):
                if (verbose == 1):
                    QueryLines=query.split('\n')
                    QueryOneLine='sql = '
                    for line in QueryLines:
                        QueryOneLine=QueryOneLine+" "+line.strip()
                    print(QueryOneLine)
                if (verbose > 1):
                    print(query)
            cur.execute(query)
            desc=[d[0].lower() for d in cur.description]

            mod_list=[]
            for row in cur:
                rowd=dict(zip(desc,row))
#                print(rowd)

                for mod in rowd['modulelist'].split(','):
                    proc_dict[proc_attempt][mod]={}
                    proc_dict[proc_attempt][mod]['execs']=[]
#                    proc_dict[proc_attempt][mod]['fail']=0
#                    proc_dict[proc_attempt][mod]['pass']=0
#                    proc_dict[proc_attempt][mod]['proc']=0
#                    proc_dict[proc_attempt][mod]['fail_jk']=[]
#                    proc_dict[proc_attempt][mod]['proc_jk']=[]
                    mod_list.append(mod)
            proc_dict[proc_attempt]['modlist']=mod_list

#
#           Now look at specific of what has been run (and check for failures)
#
#            queryitems = ["w.modname","e.task_id","e.name","t.status","j.jobkeys","t.start_time","t.end_time","t.exec_host"]
#            querylist = ",".join(queryitems)
#            coldict={}
#            for index, item in enumerate(queryitems):
#                coldict[item]=index
#            query = """select %s from %spfw_wrapper w, %spfw_exec e, %spfw_job j where w.pfw_attempt_id=%s and w.pfw_attempt_id=e.pfw_attempt_id and e.wrapnum=w.wrapnum and w.pfw_attempt_id=j.pfw_attempt_id and w.jobnum=j.jobnum  """ % ( querylist, db_Schema, db_Schema, db_Schema, AttemptID)
            query = """SELECT 
                w.modname as modname,
                e.task_id as task_id,
                e.name    as name,
                e.maxrss as maxrss,
                t.status  as status,
                j.jobkeys as jobkeys,
                (cast(t.end_time as date)-cast(t.start_time as date)) as walltime,
                (cast(SYSDATE as date)-cast(t.start_time as date)) as proctime,
                t.start_time as start_time,
                t.end_time as end_time,
                t.exec_host as exec_host
            FROM {schema:s}pfw_wrapper w, {schema:s}pfw_exec e, {schema:s}pfw_job j, {schema:s}task t 
            WHERE w.pfw_attempt_id={AttID:d} 
                and w.pfw_attempt_id=e.pfw_attempt_id 
                and e.pfw_wrapper_task_id=w.task_id
                and w.pfw_attempt_id=j.pfw_attempt_id 
                and w.pfw_job_task_id=j.task_id 
                and e.task_id=t.id 
            ORDER BY t.start_time""".format(schema=db_Schema,AttID=AttemptID)

            if (verbose > 0):
                if (verbose == 1):
                    QueryLines=query.split('\n')
                    QueryOneLine='sql = '
                    for line in QueryLines:
                        QueryOneLine=QueryOneLine+" "+line.strip()
                    print(QueryOneLine)
                if (verbose > 1):
                    print(query)
            cur.execute(query)
            desc=[d[0].lower() for d in cur.description]

            execDumpList=[]
            for row in cur:
                rowd=dict(zip(desc,row))
                execDumpList.append(rowd)
                
#                print(rowd)
                mname=rowd['modname'].lower()
                if (mname not in proc_dict[proc_attempt]):
                    print("# Warning: Missing module ",mname," (added)")
                    proc_dict[proc_attempt][mname]={}
                    proc_dict[proc_attempt][mname]['execs']=[]
                    proc_dict[proc_attempt]['modlist'].append(mname)
                ename=rowd['name'].lower()
                if (ename not in proc_dict[proc_attempt][mname]['execs']):
                    proc_dict[proc_attempt][mname][ename]={}
                    proc_dict[proc_attempt][mname][ename]['fail']=0
                    proc_dict[proc_attempt][mname][ename]['pass']=0
                    proc_dict[proc_attempt][mname][ename]['proc']=0
                    proc_dict[proc_attempt][mname][ename]['fail_jk']=[]
                    proc_dict[proc_attempt][mname][ename]['proc_jk']=[]
                    proc_dict[proc_attempt][mname][ename]['walltime']=[]
                    proc_dict[proc_attempt][mname][ename]['maxrss']=[]
                    proc_dict[proc_attempt][mname]['execs'].append(ename)

                if (rowd['status'] is not None):
                    if (rowd['status']!=0):
                        proc_dict[proc_attempt][mname][ename]['fail']=proc_dict[proc_attempt][mname][ename]['fail']+1
                        proc_dict[proc_attempt][mname][ename]['fail_jk'].append(rowd['jobkeys'])
                    else:
                        proc_dict[proc_attempt][mname][ename]['pass']=proc_dict[proc_attempt][mname][ename]['pass']+1
                        if (rowd['maxrss'] is not None):
                            proc_dict[proc_attempt][mname][ename]['maxrss'].append(rowd['maxrss']/1024.)    
                        if (rowd['walltime'] is not None):
                            proc_dict[proc_attempt][mname][ename]['walltime'].append(24.*3600.*rowd['walltime'])    
#                        print(rowd['t.start_time'],rowd['end_time'],rowd['exec_host'])
                else:
                    proc_dict[proc_attempt][mname][ename]['proc']=proc_dict[proc_attempt][mname][ename]['proc']+1
                    proc_dict[proc_attempt][mname][ename]['proc_jk'].append(rowd['jobkeys'])
#                    print(rowd['jobkeys'])
            mod_list=[]
            mod_unfinished=0
            for mod in proc_dict[proc_attempt]['modlist']:
                for ename in proc_dict[proc_attempt][mod]['execs']:
                    if ((proc_dict[proc_attempt][mod][ename]['fail']>0)or(proc_dict[proc_attempt][mod][ename]['proc']>0)or(args.full)):
                        mod_list.append(mod)
                    if ((proc_dict[proc_attempt][mod][ename]['fail']>0)or(proc_dict[proc_attempt][mod][ename]['proc']>0)):
                        mod_unfinished=mod_unfinished+1
            if (mod_unfinished==0):
                print("#  No failures or running jobs detected")


            texec_host=[execDumpList[j]['exec_host'] for j, texec in enumerate(execDumpList)]
            uniq_texec_host=list(set(texec_host))
            if (len(uniq_texec_host)>0):
                host_str="# Running on:"
                for host in uniq_texec_host:
                    if (host is not None):
                        host_str=host_str+" "+host
                print(host_str)
            else:
                print("No exec_host found")

            print("#")            
            print("#{:30s} {:20s}  {:5s} {:5s} {:5s} {:8s} {:8s} {:8s} {:8s} {:s} {:s} ".format('module','exec',' pass',' fail',' run','md(Wall)','mx(Wall)','md(mem)','mx(mem)','fail_keys','run_keys'))
            print("#{:30s} {:20s}  {:5s} {:5s} {:5s} {:8s} {:8s} {:8s} {:8s} {:s} {:s} ".format(' ','  ','  #  ','  #  ','  #','   [s]  ','   [s]  ','  [k]  ','  [k]  ','fail_keys','run_keys'))
            print("#{:30s} {:20s}  {:5s} {:5s} {:5s} {:8s} {:8s} {:8s} {:8s} {:s} {:s} ".format('------','----','-----','-----','----','--------','--------','-------','-------','---------','--------'))
            for mod in mod_list:
                num_exec=0
                for ename in proc_dict[proc_attempt][mod]['execs']:
                    num_exec=num_exec+1
                    fail_sum_jk=""
                    proc_sum_jk=""
                    if (len(proc_dict[proc_attempt][mod][ename]['walltime']) > 0):
                        tmp_wall=numpy.array(proc_dict[proc_attempt][mod][ename]['walltime'])
                        wmed=numpy.median(tmp_wall)
                        wmax=numpy.amax(tmp_wall)
                    else:
                        wmed=-1.
                        wmax=-1.
                    if (len(proc_dict[proc_attempt][mod][ename]['maxrss']) > 0):
                        tmp_mem=numpy.array(proc_dict[proc_attempt][mod][ename]['maxrss'])
                        mmed=numpy.median(tmp_mem)
                        mmax=numpy.amax(tmp_mem)
                    else:
                        mmed=-1.
                        mmax=-1.
                    if (proc_dict[proc_attempt][mod][ename]['fail']>0):
                        fail_sum_jk="Failed: " + ",".join(proc_dict[proc_attempt][mod][ename]['fail_jk'])
                    if (proc_dict[proc_attempt][mod][ename]['proc']>0):
                        proc_sum_jk="Running: " + ",".join(proc_dict[proc_attempt][mod][ename]['proc_jk'])
                    if (num_exec == 1):
                        print(" {:30s} {:20s}  {:5d} {:5d} {:5d} {:8.1f} {:8.1f} {:8.1f} {:8.1f} {:s} {:s} ".format(mod[:30],ename[:20],proc_dict[proc_attempt][mod][ename]['pass'],proc_dict[proc_attempt][mod][ename]['fail'],proc_dict[proc_attempt][mod][ename]['proc'],wmed,wmax,mmed,mmax,fail_sum_jk,proc_sum_jk))
                    else:
                        print(" {:30s} {:20s}  {:5d} {:5d} {:5d} {:8.1f} {:8.1f} {:8.1f} {:8.1f} {:s} {:s} ".format(" ",ename[:20],proc_dict[proc_attempt][mod][ename]['pass'],proc_dict[proc_attempt][mod][ename]['fail'],proc_dict[proc_attempt][mod][ename]['proc'],wmed,wmax,mmed,mmax,fail_sum_jk,proc_sum_jk))

#
#           Dump detailed output 
#
            if (args.detail is not None):
                print("# ")
                print("# ")
                print("#{mname:30s} {ename:20s} {stime:22s} {statval:3s} {memval:8s} {wallval:8s} ".format(
                        mname='  module name',
                        ename='  exec name',
                        stime=' start time',
                        statval='status',
                        memval=' mem',
                        wallval=' wall'
                        ))
                print("#{mname:30s} {ename:20s} {stime:22s} {statval:3s} {memval:8s} {wallval:8s} ".format(
                        mname='------------------------------',
                        ename='--------------------',
                        stime='----------------------',
                        statval='--------',
                        memval='--------',
                        wallval='--------'
                        ))
#
                for wexec in execDumpList:
                    if (wexec['status'] is None):
                        cstat=-1
                    else:
                        cstat=wexec['status']
                    if (wexec['maxrss'] is None):
                        cmem=-1
                    else:
                        cmem=wexec['maxrss']/1024.
                    if (wexec['walltime'] is None):
                        cwall=-1
                    else:
                        cwall=3600.*24*wexec['walltime']
                    if (wexec['start_time'] is None):
                        ctime='processing'
                    else:
                        ctime=wexec['start_time'].strftime("%Y%m%d:%H%M%S")
                    print(" {mname:30s} {ename:20s} {stime:22s} {statval:3d} {memval:8.1f} {wallval:8.1f} ".format(
                        mname=wexec['modname'][:30],
                        ename=wexec['name'][:20],
                        stime=ctime[:22],
                        statval=cstat,
                        memval=cmem,
                        wallval=cwall))
#
#                
#               print(attempt['stime'])

    exit(0)
