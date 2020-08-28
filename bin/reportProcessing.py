#!/usr/bin/env python
"""
Report processing attempts for a procecssing request, 
based on data in the  operational database.

IN general, a  processing request consisists of many
attempts.   The report print one line for each distinct
final status of the attempt. the report is ordered
by the tiem of the first attempt in the request.


Optons available via <command> --help
"""

import opstoolkit.reportquery  as rq
import opstoolkit.reportmunger as rm
import opstoolkit.reportprint  as rp

def create_run_str(reqnum, unitname, attnum):
    """ Create the standard string identifying a run to a human"""
    run = '%s_r%sp%02d' % (unitname, reqnum, int(attnum))
    return run

def unique_strings(l):
   dict = {}
   for s in l:
      dict[s] = 1
   return dict.keys()

def main(args):
   """ generate the report and write to stdout"""
   data, hdr = mkReport(args)
   writeReport(data, hdr, args)

def  writeReport (data, header, args):
    munger = rm.colMunger(data, header)
    munger.replace(lambda x : "run/abt" if type(x) == type(None) else x, "status")
    data = munger.get_data()
    data = [header] + data
    printer = rp.prettyPrinter()
    #printer.set_render_func_by_type(None,  lambda d : "run/abort")
    printer.pprint(data)

def mkReport(args):
   """ Generate the report as python table

   If a program wanted this report, this is 
   the subroutine to call, 
   """
   dbh = DesDbi(os.path.join(os.getenv("HOME"),".desservices.ini"),args.section)

   sql="""
     select 
       r.project,
       r.pipeline,
       a.operator,
       a.reqnum,
       t.status,
       a.data_state,
       /*count(*) nattempts,*/
       count (distinct a.unitname) nunits,
       min(a.unitname) firstunit,
       max(a.unitname) lastunit,
       min(a.submittime) begun,
       max(t.end_time) last_finish
     from
       prod.pfw_attempt a, prod.pfw_request r, prod.task t 
     where 
         r.reqnum = a.reqnum
      and
         a.SUBMITTIME > SYSDATE-%s
      and 
         t.id = a.task_id
     group by
      a.reqnum,
      r.project,
      r.pipeline,
      t.status,
      a.data_state,
      a.operator
     order by 
       begun, 
       t.status


   """  % args.days_back

  

   q = rq.Q(dbh, sql, args)
   q.purge_query(sql,olderthan=(1.0/24/4))
   #
   # Grr the best oracLE out of the can does is 
   # ginve a list of the datastates, but it cannot
   # give a duplicate removed list. (well I need to think more...
   data=[]
   for row in q.query_via_cache():
      data.append(row)
   hdr = q.get_header()
   return data, hdr

if __name__ == "__main__":

   import os
   import sys
   import time
   import argparse 
   from despydb import DesDbi 
   #import cx_Oracle #needed to catch execptions

   """Create command line arguments"""
   parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
   parser.add_argument('--section','-s',default='db-desoper',
             help='section in the .desservices file w/ DB connection information')
   parser.add_argument('--debug','-d',help='print debug info', default=False, action='store_true')
   parser.add_argument('--header_off','-H',help='suppress header', default=False, action='store_true')
   parser.add_argument('--detailed','-D',help='report on every trasfer_batch', default=False, action='store_true')
   parser.add_argument('--wide','-w',help='be wide -- list all srouce, nodes, etc.', default=False, action='store_true')
   parser.add_argument('--csv','-c',help='print as a CSV (def pretty print)', default=False, action='store_true')
   parser.add_argument('--use_query_cache','-C',help='use the query Cache', default=False, action='store_true')
   parser.add_argument('--days_back','-b',help='how far to got back in time(days)', default=7.0)
        
   args = parser.parse_args()


   main(args)
