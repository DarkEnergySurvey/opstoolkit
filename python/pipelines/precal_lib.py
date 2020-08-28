#! /usr/bin/env python

from os import path
from sys import exit
from despydb import DesDbi

def make_comment(date,nite,reqnum):
    comment = """
              Autosubmitted at %s
              -----
              h3. Nite: %s
              -----
              * Reqnum: %s
              * Attnum: 01

              h5. Run Status:

              h6. Tagged:
              h6. Datastate:

              h5. Location:  /archive_data/desarchive/OPS/precal/%s-r%s/p01
              """ % (date,nite,reqnum,nite,reqnum)
    return comment

class Get_Precal_Inputs():
    def __init__(self,section):
        """Connect to database using user's .desservices file"""
        dbh = DesDbi(None,section)
        self.section = section
        self.cur = dbh.cursor()

    def check_submitted(self,date):
        """Check to see if a precal has been submitted with given date"""
        was_submitted = "select count(*) from pfw_attempt where unitname= '%s'" % (date)
        self.cur.execute(was_submitted)
        count = self.cur.fetchone()[0]
        return count

    def get_max(self):
        """Get nite of max dflat"""
        max_dflat = "select max(expnum) from exposure where obstype='dome flat'"
        self.cur.execute(max_dflat)
        max_expnum = self.cur.fetchone()[0]
        fetch_nite = "select distinct nite from exposure where expnum=%s" % (max_expnum)
        self.cur.execute(fetch_nite)
        dflat_nite = self.cur.fetchone()[0]
        return max_expnum,dflat_nite
