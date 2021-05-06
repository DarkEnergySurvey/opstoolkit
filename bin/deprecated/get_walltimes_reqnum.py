#!/usr/bin/env python

from  despydb import desdbi
import os,sys
import despyastro
import numpy

desservicesfile = os.path.join(os.environ['HOME'],'.desservices.ini')
db_section = 'db-destest'
dbh = desdbi.DesDbi(desservicesfile, section=db_section)


def get_unitnames(**kw):

    QUERY = """select unique unitname,attnum from pfw_exec where reqnum={reqnum} """
    rec = despyastro.query2rec(QUERY.format(**kw), dbhandle=dbh)
    return rec

def get_walltimes(unitname,**kw):

    QUERY = """select sum(walltime) from pfw_exec where reqnum={reqnum} and attnum={attnum} and unitname='{unitname}'"""
    kw['unitname'] = unitname
    walltime = despyastro.query2dict_of_columns(QUERY.format(**kw), dbhandle=dbh)
    return walltime

if __name__ == "__main__":

    try:
        reqnum = sys.argv[1]
    except:
        usage = "ERROR:\n\tUSAGE: %s <reqnum> <attnum>" % os.path.basename(sys.argv[0])
        exit(usage)

    kw = {'reqnum':reqnum,}
    #'attnum':attnum}
    
    rec = get_unitnames(**kw)

    wtimes = []
    for u in rec:

        try:
            walltime = get_walltimes(u['UNITNAME'],reqnum=reqnum, attnum=u['ATTNUM'])
            w = walltime['SUM(WALLTIME)'][0]/3600.
            wtimes.append(w)
            print "%s p%02d %6.2f hrs" % (u['UNITNAME'],u['ATTNUM'],w)
        except:
            print "No Walltime for %s" % u['UNITNAME']

    w = numpy.array(wtimes)
    
    print "# --------------------------------"
    print "# Walltime min: %.3f hrs" % w.min()
    print "# Walltime max: %.3f hrs" % w.max()
    print "# Walltime ave: %.3f hrs" % w.mean()
