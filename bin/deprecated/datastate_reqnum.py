#!/usr/bin/env python3

"""
Simple set of tasks use to set state and tag runs
Felipe Menanteau, June 2015

Python 3 migration: RAG, Sept 2020
"""

import os,sys
import despydb.desdbi
import despyastro

def connect_DB(db_section):
    dbh = despydb.desdbi.DesDbi(section=db_section,retry=True)
    return dbh

def update_status_null(dbh,reqnum):

    UPDATE_STATUS = "update task set status=0 where status is NULL and id in (select task_id from pfw_attempt where reqnum={REQNUM})"
    QUERY_STATUS = "select distinct unitname, reqnum, attnum from pfw_attempt p, task t where reqnum={REQNUM} and t.id=p.task_id and status is null and data_state!='JUNK' order by unitname"

    cur = dbh.cursor()
    cur.execute(QUERY_STATUS.format(REQNUM=reqnum))
    a = cur.fetchall()
    N = len(a)

    if N > 0:
        print("# Found {:d} runs with NULL status -- will update to STATUS=0".format(N))
        cur.execute(UPDATE_STATUS.format(REQNUM=reqnum))
        dbh.commit()
        print("# Done")
    else:
        print("# NO runs found with NULL status")
    cur.close()
    return


def set_datastate(dbh,reqnum,new_state):

    cur = dbh.cursor()
    # If not JUNK
    if new_state != 'JUNK':
        Q_STATE = "select distinct unitname, reqnum, attnum, p.id, data_state from pfw_attempt p, task t where reqnum={REQNUM} and t.id=p.task_id and status=0 and data_state!='JUNK' order by unitname"
    # if new_state == JUNK
    else:
        Q_STATE = "select distinct unitname, reqnum, attnum, p.id, data_state from pfw_attempt p, task t where reqnum={REQNUM} and t.id=p.task_id order by unitname"

    rec = despyastro.query2rec(Q_STATE.format(REQNUM=reqnum),dbh)
    N = len(rec)
    for k in range(N):
        U_STATE = "update pfw_attempt set data_state='{new_state}' where unitname='{unitname}' and reqnum={reqnum} and attnum={attnum}"
        update = U_STATE.format(new_state=new_state, unitname=rec['UNITNAME'][k], reqnum=rec['REQNUM'][k], attnum=rec['ATTNUM'][k]) 
        print("Doing: {:s} ".format(update))
        cur.execute(update)
    cur.close()
    dbh.commit()
    return rec

def check_tag_exists(dbh,tag):

    # Check to see if the TAG exist:
    Q_TAG = "select count(TAG) from OPS_PROCTAG_DEF where TAG='{tag}'"
    cur = dbh.cursor()
    cur.execute(Q_TAG.format(tag=tag))
    (count,) = cur.fetchone()
    if count > 0:
        return

    # Check if we want to add TAG
    answer = 'yes'
    answer = raw_input('Would you like to add TAG=%s to OPS_PROCTAG_DEF?\n[YES/no]: ' % tag)
    print(answer)
    if ('no' in answer) or ('No' in answer):
        return

    # Now we try to add the TAG
    cur = dbh.cursor()
    description = raw_input('Please Enter Short TAG Description:\n')
    docurl      = raw_input('Please Enter DOCURL [Optional]:\n')
    I_TAG = "insert into OPS_PROCTAG_DEF (TAG,DESCRIPTION,DOCURL) values ('{tag}','{description}','{docurl}')"
    print("# Inserting {:s} into OPS_PROCTAG_DEF".format(tag))
    cur.execute(I_TAG.format(tag=tag,description=description,docurl=docurl))
    cur.close()
    dbh.commit()
    return

def tag_reqnum(dbh,rec,reqnum,tag):

    check_tag_exists(dbh,tag)

    cur = dbh.cursor()
    I_TAG = "insert into OPS_PROCTAG (UNITNAME,REQNUM,ATTNUM,PFW_ATTEMPT_ID,TAG) values ('{unitname}',{reqnum},{attnum},{id},'{tag}')" 

    N = len(rec)
    for k in range(N):
        insert = I_TAG.format(unitname=rec['UNITNAME'][k], reqnum=rec['REQNUM'][k], attnum=rec['ATTNUM'][k],id=rec['ID'][k],tag=tag)
        print("Doing: {:s} ".format(insert))
        cur.execute(insert)
    cur.close()
    dbh.commit()
    return

def cmdline():

    import argparse
    parser = argparse.ArgumentParser(description="Tags a given REQNUM")
    # The positional arguments
    parser.add_argument("reqnum", action="store",default=None,
                        help="Request number")

    parser.add_argument("--new_state", action="store",default=None,
                        choices=['ACTIVE','NEW','JUNK'],
                        help="New State to five")

    parser.add_argument("--no_update_status_null", action="store_false",
                        dest='update_status_null',
                        help="Do not update status NULL runs")

    parser.add_argument("--update_status_null", action="store_false",
                        dest='update_status_null',
                        help="Will update status NULL runs")

    parser.add_argument("--tag", action="store",default=None,
                        help="TAG to add to runs")

    parser.add_argument("--db_section", action="store", default='db-destest',
                        choices=['db-desoper','db-destest'],
                        help="DB Section to use")

    parser.set_defaults(update_status_null=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":

    # Get the options
    args  = cmdline()

    # Connect to DB
    dbh = connect_DB(db_section=args.db_section)

    # Update status in case of status=NULL
    if args.update_status_null:
        update_status_null(dbh,reqnum=args.reqnum)

    # Set the to ACTIVE or whatever we want
    if args.new_state:
        rec = set_datastate(dbh,reqnum=args.reqnum,new_state=args.new_state)

    # Add a TAG for these runs 
    if args.tag:
        tag_reqnum(dbh,rec,reqnum=args.reqnum,tag=args.tag)   
