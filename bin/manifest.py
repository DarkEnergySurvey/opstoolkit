#! /usr/bin/env python3
"""
Query a night to determine the observations present.  Output can be to 
STDOUT (default) and/or to a file.  Terse output (-t) is also possible, 
in which case only  a summary is printed (i.e. description of individual 
exposures is skipped.
"""

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
    import stat
    import time
    import csv
    import sys
    import datetime
    import math
    import numpy

    parser = argparse.ArgumentParser(description='Produce a summary listing (manifest) of observations for a night')
    parser.add_argument('-n', '--night',   action='store', type=str, default=None, help='Night (nite) for which to produce manifest')
    parser.add_argument('-f', '--fileout', action='store', type=str, default='-',  help='Summary listing filename (default is STDOUT)')
    parser.add_argument('-t', '--terse',   action='store_true', default=False, help='Flag to give terse summary listing (no individual file information)')
    parser.add_argument('-q', '--quiet',   action='store_true', default=False, help='Flag to run as silently as possible')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section', action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)

    if (args.night is None):
        print("Must specify night (-n) for query")
        exit(1)
    nite=args.night

    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

#
#   Define a dictionary to translate filter/band to an ordinate 
#
    band2i={"u":0,"g":1,"r":2,"i":3,"z":4,"Y":5,"VR":6}
    band_allow=["u","g","r","i","z","Y","VR"]

    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
    cur = dbh.cursor()

#
#   Main (workhorse) query to collect exposure information
#
    queryitems = ["e.filename", "e.expnum", "e.nite", "e.date_obs", "e.time_obs", "e.mjd_obs", "e.obstype", "e.band", "e.object", "e.telra", "e.teldec", "e.tradeg", "e.tdecdeg", "e.airmass", "e.exptime", "e.propid", "e.program", "e.field" ]
    querylist = ",".join(queryitems)
    query = """select %s from %sexposure e where e.nite='%s' order by e.mjd_obs  """ % ( querylist, db_Schema, nite )
    querylist = ",".join(queryitems)
    coldict={}
    for index, item in enumerate(queryitems):
         coldict[item]=index

    query = """select %s from %sexposure e where e.nite='%s' order by e.mjd_obs  """ % ( querylist, db_Schema, nite )
    
    if args.verbose:
        print(query)
    cur.arraysize = 1000 # get 1000 at a time when fetching
    cur.execute(query)

    has_SN_obs=False
    exp_list=[]
    propid_list=[]
    lastmjd=-1.
    for item in cur:
#       print(item)
        tmp_expdict={}
        tmp_expdict["expnum"]=int(item[coldict["e.expnum"]])
        tmp_expdict["filename"]=item[coldict["e.filename"]]
        tmp_expdict["dateobs"]=item[coldict["e.date_obs"]]
        tmp_expdict["obstype"]=item[coldict["e.obstype"]]
        if (item[coldict["e.band"]] is None):
            tmp_expdict["band"]='NA'
        else:
            tmp_expdict["band"]=item[coldict["e.band"]]
        tmp_expdict["exptime"]=float(item[coldict["e.exptime"]])
        tmp_expdict["telra"]=item[coldict["e.telra"]]
        tmp_expdict["teldec"]=item[coldict["e.teldec"]]
        tmp_expdict["ra"]=float(item[coldict["e.tradeg"]])
        tmp_expdict["dec"]=float(item[coldict["e.tdecdeg"]])
        if (item[coldict["e.airmass"]] is None):
            tmp_expdict["airmass"]="%6s" % ("None")
        else:
            if (math.isnan(item[coldict["e.airmass"]])):
                tmp_expdict["airmass"]="%6s" % ("NaN")
            else:
                tmp_expdict["airmass"]="%6.3f" % float(item[coldict["e.airmass"]])
        tmp_expdict["propid"]=item[coldict["e.propid"]]
        propid_list.append(item[coldict["e.propid"]])
        if (item[coldict["e.object"]] is None):
            tmp_expdict["object"]="null"
        else:
            tmp_expdict["object"]=item[coldict["e.object"]]
        if (item[coldict["e.program"]]=="survey"):
            tmp_expdict["program"]='survey'
        elif (item[coldict["e.program"]]=="supernova"):
            tmp_expdict["program"]='SN'
            has_SN_obs=True
        elif (item[coldict["e.program"]]=="photom-std-field"):
            tmp_expdict["program"]='phot-std'
        else:
            if (tmp_expdict["obstype"] in ['zero','dark','dome flat','sky flat']):
                tmp_expdict["program"]='cal'
            else:
                tmp_expdict["program"]='unknown'
        tmp_expdict["field"]=item[coldict["e.field"]]
        tmp_expdict["mjd_obs"]=float(item[coldict["e.mjd_obs"]])
        if (lastmjd == -1):
            tmp_expdict["elast"]=0.0
        else:
            tmp_expdict["elast"]=3600.*24.*(tmp_expdict["mjd_obs"]-lastmjd)
        lastmjd=tmp_expdict["mjd_obs"]
        exp_list.append(tmp_expdict)

    uniq_propid=list(set(propid_list))

#
#   Since we have made it this far the query probably succeeded.  
#   Go ahead and open the output file if that was requested.
#
    write_file=False
    if (args.fileout != "-"):
        fout = open(args.fileout,"w")
        write_file=True
#
#   Check for prop-ids returned by the query
#
    first=0
    for qprop in uniq_propid:
        sum=0
        for erec in exp_list:
            if (erec["propid"] == qprop):
                sum=sum+1
        if (first == 0):
            if (write_file):
                fout.write("#SUM                PROPOSAL(S): {:s} ({:d}) \n".format(qprop,sum))
            if (not(args.quiet)):
                print("#SUM                PROPOSAL(S): {:s} ({:d}) ".format(qprop,sum))
            first=first+1
        else:
            if (write_file):
                fout.write("#SUM                             {:s} ({:d}) ".format(qprop,sum))
            if (not(args.quiet)):
                print("#SUM                             {:s} ({:d}) ".format(qprop,sum))

    if (write_file):
        fout.write("#SUM \n")
    if (not(args.quiet)):
        print("#SUM ") 
#
#   Check for zeros, darks, flats, objects, 
#
    num_zero=0
    num_dark=0
    num_dflat=numpy.zeros(7,dtype=numpy.int32)
    num_tflat=numpy.zeros(7,dtype=numpy.int32)
    num_obj=numpy.zeros(7,dtype=numpy.int32)
    nfsum=0
    ntsum=0

    obssum=0
    othersum=0
    delist="none"
    for erec in exp_list:
        if (erec["obstype"] == "zero"):
            num_zero=num_zero+1
        elif (erec["obstype"] == "dark"):
            num_dark=num_dark+1
            if (num_dark == 1):
                delist='exptime='+'%.1f' % erec["exptime"]
            else:
                delist=delist+','+'%.1f' % erec["exptime"]
        elif (erec["obstype"] == "dome flat"):
            nfsum=nfsum+1
            if (erec["band"] in band_allow):
                num_dflat[band2i[erec["band"]]]=num_dflat[band2i[erec["band"]]]+1
        elif (erec["obstype"] == "sky flat"):
            ntsum=ntsum+1
            if (erec["band"] in band_allow):
                num_tflat[band2i[erec["band"]]]=num_tflat[band2i[erec["band"]]]+1
        elif ((erec['obstype'] == "object")or(erec['obstype']=="standard")):
            obssum=obssum+1
            if (erec["band"] in band_allow):
                num_obj[band2i[erec["band"]]]=num_obj[band2i[erec["band"]]]+1
        else:
            othersum=othersum+1

#    dfsum=ufsum+gfsum+rfsum+ifsum+zfsum+yfsum+nfsum
#    dtsum=utsum+gtsum+rtsum+itsum+ztsum+ytsum+ntsum

    dflat_string=[]
    tflat_string=[]
    nobj_string=[]
    for band in band_allow:
        dflat_string.append("%s=%d"%(band,num_dflat[band2i[band]]))
        tflat_string.append("%s=%d"%(band,num_tflat[band2i[band]]))
        nobj_string.append("%s=%d"%(band,num_obj[band2i[band]]))
    if (nfsum-numpy.sum(num_dflat) > 0):
        dflat_string.append("other=%d"%(nfsum-numpy.sum(num_dflat)))
    if (ntsum-numpy.sum(num_tflat) > 0):
        tflat_string.append("other=%d"%(ntsum-numpy.sum(num_tflat)))
    if (obssum-numpy.sum(num_obj) > 0):
        nobj_string.append("other=%d"%(obssum-numpy.sum(num_obj)))

    if (write_file):
        fout.write("#SUM        zero: {:5d} \n".format(num_zero))
        fout.write("#SUM        dark: {:5d} ({:s}) \n".format(num_dark,delist))
        fout.write("#SUM   dome flat: {:5d} ({:s}) \n".format(nfsum,(",".join(dflat_string))))
        fout.write("#SUM    sky flat: {:5d} ({:s}) \n".format(ntsum,(",".join(tflat_string))))
        fout.write("#SUM      object: {:5d} ({:s}) \n".format(obssum,(",".join(nobj_string))))
        fout.write("#SUM       other: {:5d} \n".format(othersum))
        fout.write("#SUM \n") 
    if (not(args.quiet)):
        print("#SUM        zero: {:5d} ".format(num_zero))
        print("#SUM        dark: {:5d} ({:s}) ".format(num_dark,delist))
        print("#SUM   dome flat: {:5d} ({:s}) ".format(nfsum,(",".join(dflat_string))))
        print("#SUM    sky flat: {:5d} ({:s}) ".format(ntsum,(",".join(tflat_string))))
        print("#SUM      object: {:5d} ({:s}) ".format(obssum,(",".join(nobj_string))))
        print("#SUM       other: {:5d} ".format(othersum))
        print("#SUM ") 

#
#   If SN observations then provide summaries for each field.
#
    if (has_SN_obs):
        for SNfld in ['SN-C1','SN-C2','SN-C3','SN-E1','SN-E2','SN-S1','SN-S2','SN-X1','SN-X2','SN-X3']:
            nssum=0
            num_snobs=numpy.zeros(7,dtype=numpy.int32)
            for erec in exp_list:
                if (erec["program"]=='SN'):
                    if (erec["field"]==SNfld):
                        nssum=nssum+1
#
#                       Exptime > 30.0 used to remove pointing exposure (but is kept as "other")
#
                        if ((erec["band"] in band_allow)and(erec["exptime"]>30.0)):
                            num_snobs[band2i[erec["band"]]]=num_snobs[band2i[erec["band"]]]+1
            if (nssum > 0):
                if (write_file):
                    fout.write("#SUM       {:5s}: {:5d} (u={:d},g={:d},r={:d},i={:d},z={:d},Y={:d},other={:d}) \n".format(SNfld,nssum,num_snobs[0],num_snobs[1],num_snobs[2],num_snobs[3],num_snobs[4],num_snobs[5],nssum-numpy.sum(num_snobs)))
                if (not(args.quiet)):
                    print("#SUM       {:5s}: {:5d} (u={:d},g={:d},r={:d},i={:d},z={:d},Y={:d},other={:d}) ".format(SNfld,nssum,num_snobs[0],num_snobs[1],num_snobs[2],num_snobs[3],num_snobs[4],num_snobs[5],nssum-numpy.sum(num_snobs)))
        if (write_file):
            fout.write("#SUM \n") 
        if (not(args.quiet)):
            print("#SUM ") 
    else:
        if (write_file):
            fout.write("#SUM No Supernova Observations Identified \n")
            fout.write("#SUM \n") 
        if (not(args.quiet)):
            print("#SUM No Supernova Observations Identified ")
            print("#SUM ") 
    
#
#   line by line summary of each exposure's information
#
    if (not(args.terse)):
        if (write_file):
            fout.write("#SUM  exposure                                                               exposure  since                       AIR             PROPOSAL \n")
            fout.write("#SUM    id      File Name            date_obs              obstype    filter   time    last   TEL_RA    TEL_DEC   MASS  PROGRAM      ID          OBJECT \n")
            fout.write("#SUM--------------------------------------------------------------------------------------------------------------------------------------------------- \n")
        if (not(args.quiet)):
            print("#SUM  exposure                                                               exposure  since                       AIR             PROPOSAL ")
            print("#SUM    id      File Name            date_obs              obstype    filter   time    last   TEL_RA    TEL_DEC   MASS  PROGRAM      ID          OBJECT ")
            print("#SUM--------------------------------------------------------------------------------------------------------------------------------------------------- ")
        if (len(exp_list) > 0):
            last_expnum=exp_list[0]["expnum"]-1
        for erec in exp_list:
#            print(erec['expnum'])
            if (write_file):
                if (last_expnum != (erec["expnum"]-1)):
                    fout.write("# \n")
                fout.write(" {:10d} {:22s} {:21s} {:12s} {:6s} {:6.1f} {:7.1f} {:9.5f} {:9.5f} {:5s} {:9s}  {:10s} {:s} \n".format(
                erec["expnum"],erec["filename"],erec["dateobs"][0:21],erec["obstype"],erec["band"][0:5],erec["exptime"],erec["elast"],erec["ra"],erec["dec"],erec["airmass"],erec["program"],erec["propid"],erec["object"]))
            if (not(args.quiet)):
                if (last_expnum != (erec["expnum"]-1)):
                    print("# ")
#                print(erec["expnum"],erec["filename"],erec["dateobs"][0:21],erec["obstype"],erec["band"][0:5],erec["exptime"],erec["elast"],erec["ra"],erec["dec"],erec["airmass"],erec["program"],erec["propid"],erec["object"])
                print(" {:10d} {:22s} {:21s} {:12s} {:6s} {:6.1f} {:7.1f} {:9.5f} {:9.5f} {:5s} {:9s}  {:10s} {:s} ".format(
                erec["expnum"],erec["filename"],erec["dateobs"][0:21],erec["obstype"],erec["band"][0:5],erec["exptime"],erec["elast"],erec["ra"],erec["dec"],erec["airmass"],erec["program"],erec["propid"],erec["object"]))
            last_expnum=erec["expnum"]

    if (write_file):
        fout.close()

    exit(0)

