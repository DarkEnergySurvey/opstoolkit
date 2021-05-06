#!/usr/bin/env python3

import despydb.desdbi
import os,sys
import despyastro
import numpy

desservicesfile = os.path.join(os.environ['HOME'],'.desservices.ini')
db_section = 'db-desoper'
dbh = despydb.desdbi.DesDbi(desservicesfile, section=db_section)


def get_unitnames(**kw):

    QUERY = """select unique unitname from pfw_attempt where reqnum={reqnum} and attnum={attnum}"""
    UNITNAMES = despyastro.query2rec(QUERY.format(**kw), dbhandle=dbh)
    return UNITNAMES

def get_walltimes(unitname,**kw):

    QUERY = """select 24.0*3600.*(cast(t.end_time as date)-cast(t.start_time as date)) as walltime from pfw_attempt a, task t where a.reqnum={reqnum} and a.attnum={attnum} and a.unitname='{unitname}' and a.task_id=t.id"""
    kw['unitname'] = unitname
    walltime = despyastro.query2dict_of_columns(QUERY.format(**kw), dbhandle=dbh)
    return walltime

if __name__ == "__main__":

    try:
        reqnum = sys.argv[1]
        attnum = sys.argv[2]
    except:
        usage = "ERROR:\n\tUSAGE: %s <reqnum> <attnum>" % os.path.basename(sys.argv[0])
        exit(usage)

    kw = {'reqnum':reqnum,
          'attnum':attnum}
    
    unitnames = get_unitnames(**kw)

    wtimes = []
    for u in unitnames:
        try:
            walltime = get_walltimes(u['UNITNAME'],**kw)
            w = walltime['WALLTIME'][0]/3600.
            wtimes.append(w)
            print(u['UNITNAME'],w)
        except:
            print("No Walltime for {:s}".format(u['UNITNAME']))

    w = numpy.array(wtimes)
    
    print("# --------------------------------")
    print("# Number runs: {:d} ".format(w.size))
    print("# Walltime min: {:.3f} hrs".format(w.min()))
    print("# Walltime max: {:.3f} hrs".format(w.max()))
    print("# Walltime avg: {:.3f} hrs".format(w.mean()))
    print("# Walltime med: {:.3f} hrs".format(numpy.median(w)))
    print("# Walltime std: {:.3f} hrs".format(w.std()))
