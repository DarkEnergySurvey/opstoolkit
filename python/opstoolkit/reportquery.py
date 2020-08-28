"""
Common utilites for reporting
"""
import sys
import time
import hashlib


CURSOR=1
DATAFRAME=2

def print_timing(func):
    def wrapper(*arg):
        t1 = time.time()
        res = func(*arg)
        t2 = time.time()
        print >> sys.stdout, '%s took %0.3f ms' % (func.func_name, (t2-t1)*1000.0)
        return res
    return wrapper


#####################################
# SQL defining/using the query cache
#####################################
# detect if the query is alreay cached
CACHED_QUERY_DETECT_SQL="SELECT count(*)  FROM user_tables WHERE table_name = '%s'"

# create the cache index table, if not already created.
CACHE_CREATE_SQL="""DECLARE cnt NUMBER;
BEGIN
  SELECT count(*) INTO cnt FROM user_tables WHERE table_name = 'QCACHE_INDEX';
  IF cnt = 0 THEN 
    EXECUTE IMMEDIATE 'CREATE TABLE  QCACHE_INDEX 
        ( tablename varchar2(32) PRIMARY KEY, 
          created DATE,
          expires DATE)
         '; 
  END IF;
END; """

# register this query into the cache
CACHE_REGISTER_QUERY="""
MERGE INTO QCACHE_INDEX USING dual ON ( TABLENAME='%s' )
WHEN MATCHED THEN UPDATE SET CREATED = SYSDATE, EXPIRES = SYSDATE+%s
WHEN NOT MATCHED THEN INSERT (TABLENAME,CREATED,EXPIRES) 
    VALUES ('%s', SYSDATE, SYSDATE+%s )
""" 

# support purging expired entries -- get old tables, drop table, drop index entry 
STALE_CACHE_TABLES_SQL = """select TABLENAME from QCACHE_INDEX where EXPIRES < SYSDATE"""
DROP_CACHE_TABLE_SQL = """DROP TABLE %s"""
DELETE_CACHE_INDEX_ROW_BY_TABLENAME =  """DELETE FROM QCACHE_INDEX where TABLENAME = '%s'"""
# support applications purging based on time since cache entry
NHOWOLDERTHAN="SELECT count(*) cnt FROM qcache_index WHERE tablename = '%s' AND created+%s < SYSDATE"
# (tablename, olderthan)

#  support purgiing the whole cache down
#  by returning all table names
ALL_CACHE_TABLE_SQL="""
    SELECT  
      table_name  
    FROM user_tables
    WHERE  
      table_name 
    LIKE 
    'QCACHE_%'
"""


class Q(object):
   """ holds state for a query 
  
       provide the programming elegance of returning 
       an interable with tight notation, but provide
       a way to get query meta-data (like headers)

       class Q support program debug by suporting
       printing of queries and queries timing to
       stdout.

       The class method Q.query_via_cache passed
       queries thoug a cache kept in your mydb.
       a series of tables names QCACHE_ consititue
       the cache.  Queries are cahched for 4.0
       dats by default. The lifetime of newly
       created cache entries is 4.0 days. 

       ** need to close the cursor! wrong semantics
   """
   def __init__(self, dbh, query, args):
      self.cur = dbh.cursor()
      self._header = None
      self.query=query
      self.args=args
      self.expire_days=4.0 #lifetime of data in cache in days 

   def query_via_cache(self):
      """ dump query results into cache, return cursor to query"""
      if not self.args.use_query_cache:  #caching is a new and experimental feature     
          return self._query(self.query, self.args)
      args=self.args
      query=self.query
      # make the cache table if it is not there
      self._query(CACHE_CREATE_SQL, args)
      #purge any old entries
      self.purge_stale_cache()
      #all queries are from the cache prepare the table name 
      # and the eventual query)
      cached_table_name = self._table_name(query)
      cached_query = "SELECT * FROM %s" %  cached_table_name

      # is cached is the number of tables consistent with the hashed query name
      isCached=self._query(CACHED_QUERY_DETECT_SQL % cached_table_name, args).fetchall()[0][0]
      assert isCached < 2    #must be 0 or 1, ir the system is not sane
      if not isCached:
         # cache the query, then feed user from cache
         #  select into the cache
         #  register the query in the cache
         #  setup user service from the cache
         # get a unique table name 
         query_prefix = "CREATE TABLE %s  AS " % cached_table_name
         cache_load_query = query_prefix + query
         # load the user's query into cache
         self._query(cache_load_query, args)
         # create a recod of the cached table.
         self._register_cache(cached_table_name, args)
         # now prepare query srring from cache into cache -- we'll query later)
      
      #execute cached_query and return the result to user
      cur = self._query(cached_query, args)
      return cur

   def _table_name(self, query):
      """ Produce a tablename to hold the cached query results """      
      return "QCACHE_" + hashlib.sha224(query).hexdigest()[0:18].upper()

   def _register_cache(self, tablename, args):
      """ make entry for the queue in the QCAHE_INDEX table"""
      q = CACHE_REGISTER_QUERY  % (
         tablename, self.expire_days,tablename, self.expire_days)
      self._query(q, args)


   def _query(self, query, args):
      """ do query and return cursor print debug info when desired"""
      t0 = time.time()
      if args.debug : print >>sys.stderr, query
      self.cur.execute(query)
      if args.debug : print >>sys.stderr, "took %s seconds" % (time.time()-t0)
      if self.cur.description : self._header = zip(*self.cur.description)[0]
      return self.cur

   def q(self,returntype=CURSOR):
      """ perform query, return curssor, cahce header in case desired"""
      cursor = self._query(self.query, self.args)
      if returntype == CURSOR:
          return cursor
      elif returntype == DATAFRAME:
          import pandas as pd
          #pd.DataFrame.from_records(data, index=None, exclude=None, columns=None, coerce_float=False, nrows=None)
          df=pd.DataFrame.from_records(cursor.fetchall(), index=None, exclude=None, columns=self._header, coerce_float=False, nrows=None)
          return df
      else:
          Exception("query return type is not CURSOR of DATAFRAME")
   
   def delete_cache(self):
      """ completely delete all your perosnal query cahce tables"""
      if not self.args.use_query_cache : return
      for table in self._query(ALL_CACHE_TABLE_SQL, self.args).fetchall():
         self._query("DROP TABLE %s" % table , self.args)
   
   def purge_stale_cache(self):
      """  remove all stale cache entries"""
      if not self.args.use_query_cache : return
      for table in self._query(STALE_CACHE_TABLES_SQL, self.args):
         self.purge_query(table[0])
   
   def purge_query(self, query, olderthan=0):
      """ remove a specific query from cache

          kwarg olderthan as a flat indicating the deletion is conditional
          on the chachced results being older than older than  days 

           Oracle 
           throws cx_Oracle.DatabaseError: ORA-00942: table or view does not exist
           we catch this, and silenelty succeed.
      """
      if not self.args.use_query_cache : return
      tablename = self._table_name(query)
      isOlder=self._query(NHOWOLDERTHAN % (tablename, olderthan), self.args).fetchall()[0][0]
      if not isOlder: return #def purge_query(self, query, olderthan=0): nothing to purge
      #rece condition protection
      try:
         self._query(DROP_CACHE_TABLE_SQL % tablename, self.args)
         self._query(DELETE_CACHE_INDEX_ROW_BY_TABLENAME % tablename, self.args)
      except cx_Oracle.DatabaseError:
         if 'ORA-00942' not in "%s" % sys.exc_info()[1] :
            raise
      
   def get_header(self):
      """ return list of column headers from the query """
      return [ h for h in self._header]
 


def main(args):
   """ tests"""

   dbh = despydb.DesDbi(os.path.join(os.getenv("HOME"),".desservices.ini"),args.section) 
   sql="select SYSDATE d from DUAL"
   q=Q(dbh, sql, args)
   q.delete_cache()
   print "nest tewo results should be the same if cacheing"
   print q.query_via_cache().fetchall()
   time.sleep(2)
   q=Q(dbh, sql, args)
   print q.query_via_cache().fetchall()
   q.purge_query(sql)
   print "nest result should be different"
   time.sleep(2)
   q=Q(dbh, sql, args)
   print q.query_via_cache().fetchall()
   time.sleep(2)
   print "next result should be different"
   q.purge_query(sql, olderthan=0.0)
   q.purge_query("dog")  # invalid query 
   print q.query_via_cache().fetchall()

   print "dataframe stuff"
   q=Q(dbh, sql, args)
   df = q.q(returntype=DATAFRAME)
   print df
if __name__ == "__main__":

   import os
   import sys
   import time
   import datetime
   import argparse 
   import despydb
   import cx_Oracle #needed to catch execptions

   """Create command line arguments"""
   parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
   parser.add_argument('--section','-s',default='db-desoper',
             help='section in the .desservices file w/ DB connection information')
   parser.add_argument('--debug','-d',help='print debug info', default=False, action='store_true')
   parser.add_argument('--header_off','-H',help='suppress header', default=False, action='store_true')
   parser.add_argument('--detailed','-D',help='report on every trasfer_batch', default=False, action='store_true')
   parser.add_argument('--wide','-w',help='be wide -- list all srouce, nodes, etc.', default=False, action='store_true')
   parser.add_argument('--csv','-c',help='print as a CSV (def pretty print)', default=False, action='store_true')
   parser.add_argument('--use_query_cache','-C',help='use the query cache feature', default=False, action='store_true')
   parser.add_argument('--days_back','-b',help='how far to got back in time(days)', default=1.0)
        
   args = parser.parse_args()

   main(args)

