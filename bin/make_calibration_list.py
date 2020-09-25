#! /usr/bin/env python3
"""
Query a series of nights to determine the calibrations observations present 
that are appropriate for building an input list for precal and supercal pipelines.

Syntax:
    make_calibration_list -v [-s section] -f night1 -l night2

    night1 = first archive night to be probed (current in DTS).
    night2 = last  archive night to be probed (current in DTS).

    Three modes are now possible.
       1) if only -f {date} is specified only that date is used
       2) if -f and -l are specified then all exposures in that date range are considered
       3) if -w is used then search is performed starting with that date and then 
            succesively including dates before and after to reach a minimum number of 
            desired exposures (--nummin) for each calibration (currently the number of 
            u-band flats is not considered).  The -f and -l options can be used to restrict 
            range from growing beyond a certain point.
     
Options:
       -v Verbose.
       -s Section of desdbi file.

"""

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi
    from opstoolkit import nite_strings
    import re
    import stat
    import time
    import sys
    import datetime
    import numpy
   
    parser = argparse.ArgumentParser(description='Query a series of nights to determine calibrations present for precal/supercal pipelines.')
    parser.add_argument('-f', '--first',    action='store', type=str, default=None, help='First night to be included in listing')
    parser.add_argument('-w', '--workfrom', action='store', type=str, default=None, help='Results in search working outward form this date to reach a --nummin exposures for each cal')
    parser.add_argument('-l', '--last',     action='store', type=str, default=None, help='Last night to be included in listing')
    parser.add_argument('-m', '--max_comb', action='store', type=str, default=None, help='Maximum number of exposures to be combined into an individual supercal (default=150)')
    parser.add_argument('-n', '--num_min',  action='store', type=str, default=None, help='Minimum number of exposures to be combined (for search using -w option).  (Default not used)')
    parser.add_argument('-o', '--output',   action='store', type=str, default=None, help='Summary listing filename (default is STDOUT)')
    parser.add_argument('-t', '--t_crit',   action='store', type=str, default=None, help='Overhead time (in seconds) allowed for a sequence to be contiguous (Default=60)')
    parser.add_argument('-i', '--idlist',   action='store_true', default=False,     help='Do not provide exposure list but simply write list of exposure IDs (Default=False)')
    parser.add_argument('-c', '--count_only', action='store_true', default=False,   help='Do not provide exposure list but simply counts of frames found (Default=False)')
    parser.add_argument('-e', '--exclude_file', action='store', type=str, default=None, help='File containing a list of exposure numbers (one per line) to exclude from consideration.')
#    parser.add_argument('-t', '--terse',   action='store_true', default=False, help='Flag to give terse summary listing (no individual file information)')
#    parser.add_argument('-q', '--quiet',   action='store_true', default=False, help='Flag to run as silently as possible')
    parser.add_argument('-v', '--verbose',  action='store_true', default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section',  action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',   action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)
    
    if ((not(args.workfrom is None))and(args.num_min is None)):
        print("Error: outward search requires a value specified with --num_min (-n) ")
        exit(1)

    if (args.workfrom is None):
        outsearch=False
    else:
        outsearch=True
        start_night=args.workfrom
        min_num=int(args.num_min)

    exclude_list=[]
    if (not(args.exclude_file is None)):
        if os.path.isfile(args.exclude_file):
            f1=open(args.exclude_file,'r')
            for line in f1:
                line=line.strip()
                columns=line.split()
                if (columns[0] != "#"):
                    exclude_list.append(int(columns[0]))
            f1.close()
        else:
            print("Warning: Exclude file {:s} not found!  Exclude list ignored".format(args.exclude_file))
    #print exclude_list

    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

    if (outsearch):
        night1=args.workfrom
        night2=args.workfrom
        print("#      Initial range {:s} {:s} ".format(night1,night2))
    else:
        night1=args.first
        if (args.last is None):
            night2=args.first
        else:
            night2=args.last
#
#   Populate tcrit/max_comb
#
    if (args.t_crit is None):
        t_crit=60.0
    else:
        t_crit=float(args.t_crit)
    if (args.max_comb is None):
        max_comb=150
    else:
        max_comb=int(args.max_comb)

#
#   Define a dictionary to translate filter/band to an ordinate 
#
    band2i={"u":0,"g":1,"r":2,"i":3,"z":4,"Y":5,"VR":6,"zero":7}

#
#   Check for DB services 
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
    cur = dbh.cursor()
    
    ###############################################################################
    # Query a night range
    #
    queryitems = ["e.nite", "e.date_obs", "e.time_obs", "e.mjd_obs", 
          "e.obstype", "e.band", "e.object", "e.tradeg", "e.tdecdeg", 
          "e.airmass", "e.exptime", "e.propid", 
          "e.filename", "e.expnum"]
    querylist = ",".join(queryitems)
    coldict={}
    for index, item in enumerate(queryitems):
        coldict[item]=index

    getmoredata=True
    while (getmoredata):

        query = """select %s from %sexposure e where e.nite >= %s and e.nite <= %s 
        order by e.mjd_obs  """ % ( querylist, db_Schema, night1, night2 )
    
        if args.verbose:
            print(query)
        cur.arraysize = 1000 # get 1000 at a time when fetching
        cur.execute(query)

        exp_record=[]
        lastmjd=-1.
        last_seq=0
        last_etime=0
        last_type=""
        last_band=""
        for item in cur:
            tmp_expdic={}
            tmp_expdic["filename"]=re.sub(".fits","",item[coldict["e.filename"]])
            tmp_expdic["expnum"]=int(item[coldict["e.expnum"]])
            tmp_expdic["date_obs"]=item[coldict["e.date_obs"]]
            tmp_expdic["obstype"]=item[coldict["e.obstype"]]
            if (item[coldict["e.band"]] is None):
                tmp_expdic["band"]="NA"
            else:
                tmp_expdic["band"]=item[coldict["e.band"]][0:5]
            tmp_expdic["exptime"]=float(item[coldict["e.exptime"]])
            tmp_expdic["ra"]=float(item[coldict["e.tradeg"]])
            tmp_expdic["dec"]=float(item[coldict["e.tdecdeg"]])
            if (item[coldict["e.airmass"]] is None):
                tmp_expdic["airmass"]=-1.0
            else:
                if (item[coldict["e.airmass"]] in ['NaN','Nan','nan','NAN']):
                    tmp_expdic["airmass"]=-1.0
                else:
                    tmp_expdic["airmass"]=float(item[coldict["e.airmass"]])
            tmp_expdic["project"]='NOTUSED'
            tmp_expdic["propid"]=item[coldict["e.propid"]]
            if (item[coldict["e.object"]] is None):
                tmp_expdic["object"]="None"
            else:
                tmp_expdic["object"]=item[coldict["e.object"]]
#
#           Compute time since previous exposure
#
            mjd_obs=item[coldict["e.mjd_obs"]]
            if (lastmjd == -1):
                tmp_expdic["elapsed"]=0.0
            else:
                tmp_expdic["elapsed"]=3600.*24.*(mjd_obs-lastmjd)
            lastmjd=mjd_obs

#
#           Determine whether exposure is part of a sequence
#           First get rid of object frames by naming them to sequence=-1
#           If an exclude list was present also name them as sequence=-1
#

            if (tmp_expdic["obstype"]=="object"):
                tmp_expdic["seqid"]= -1
            elif (tmp_expdic["expnum"] in exclude_list):
                tmp_expdic["seqid"]= -1
            else:
                if (tmp_expdic["elapsed"]>(last_etime+t_crit)):
                    last_seq=last_seq+1
                else:
                    if (tmp_expdic["obstype"]!=last_type):
                        last_seq=last_seq+1
                    else:
                        if ((tmp_expdic["obstype"]=="dome flat")and(tmp_expdic["band"]!=last_band)):
                            last_seq=last_seq+1
                tmp_expdic["seqid"]=last_seq
            last_etime=item[coldict["e.exptime"]]
            last_type=item[coldict["e.obstype"]]
            last_band=item[coldict["e.band"]]
            exp_record.append(tmp_expdic)
#
#       Determine how long each sequence is...
#       Get rid of first frame in each calibration sequence (could switch to only happen with zero/bias)
#
        min_seq=5
        num_seq=numpy.zeros(last_seq,dtype=numpy.int32)
        new_record=[]
        for exp_rec in exp_record:
            iseq=exp_rec["seqid"]
            if (iseq > 0):
                num_seq[iseq-1]=num_seq[iseq-1]+1
#               if ((num_seq[iseq-1]==1)and(exp_rec["obstype"]=="zero")):
            if (num_seq[iseq-1]==1):
                exp_rec["seqid"]=-1
            new_record.append(exp_rec)
        del exp_record
        exp_record=new_record

#
#       Remove dome flat exposures that have long exposures (for linerarity tests) 
#       so that they don't contaminate the flats.   Currently the only known case are
#       r-band flats taken with EXP=20 (therefore cut for > EXPTIME > r_explim)
#
#       Eliminate these records and any remaining that have seqid=-1
#
        r_explim=15.
        new_record=[]
        for exp_rec in exp_record:
            if ((exp_rec["obstype"]=="dome flat")and(exp_rec["band"]=="r")and(exp_rec["exptime"]>r_explim)):
                exp_rec["seqid"]=-1
            if ((exp_rec["obstype"]=="dome flat")and("PTC" in exp_rec["object"])):
                exp_rec["seqid"]=-1
            if (exp_rec["seqid"] > 0):
                new_record.append(exp_rec)
        del exp_record
        exp_record=new_record

#       Mark records in sequence with fewer than min_seq exposures as seqid=-1
#       Eliminate records that have previously had seqid=-1

        new_record=[]
        for exp_rec in exp_record:
            iseq=exp_rec["seqid"]
            if (iseq > 0):
                if (num_seq[iseq-1] < min_seq):
                    exp_rec["seqid"]=-1
                new_record.append(exp_rec)
        del exp_record
        exp_record=new_record
            
#       Determine: total number of each calibration type (bias, flat/band) and average seq length for each
#       Eliminate records that have previously had seqid=-1

        new_record=[]
        num_cal=numpy.zeros(8,dtype=numpy.int32)
        for exp_rec in exp_record:
            iseq=exp_rec["seqid"]
            if (iseq > 0):
                iband=-1
                if (exp_rec["obstype"]=="dome flat"):
                    iband=band2i[exp_rec["band"]]
                elif (exp_rec["obstype"]=="zero"):
                    iband=band2i["zero"]
                if (iband > -1):
                    num_cal[iband]=num_cal[iband]+1
                new_record.append(exp_rec)
        del exp_record
        exp_record=new_record
        del new_record

#
#       If an outward search has been specified determine whether the 
#       minimum number of exposures has been reached
#
        if (outsearch):
            num_ok=0
            for band in ["g","r","i","z","Y"]:
                if (num_cal[band2i[band]]>=min_num):
                    num_ok=num_ok+1
            if (num_cal[band2i["zero"]]>=min_num):
                    num_ok=num_ok+1
            if (num_ok > 5):
#
#           Then we are done...
#
                getmoredata=False
            else:
#
#               Check to see whether the bounds have been reached... if not step outward
#
                if (not(args.first is None)):
                    if (night1 > args.first):
                        new_night1=nite_strings.decrement_nite(night1)
                    else:
                        new_night1=night1
                else:
                    new_night1=nite_strings.decrement_nite(night1)

                if (not(args.last is None)):
                    if (night2 < args.last):
                        new_night2=nite_strings.increment_nite(night2)
                    else:
                        new_night2=night2
                else:
                    new_night2=nite_strings.increment_nite(night2)
                if ((new_night2 == night2)and(new_night1 == night1)):
                    getmoredata=False
                else:
                    night1=new_night1
                    night2=new_night2
                    if (args.verbose):
                        print("# Expanding range to {:s} {:s} ".format(night1,night2))

        else:
            getmoredata=False
        
    if (args.verbose):
        print("#   Final range used {:s} {:s} ".format(night1,night2))
#
#   Determine whether there are too many exposures (for combination in calibration routines)
#   If so then iteratively try to remove them evenly across the input set.
#

    if ((args.verbose)or(args.count_only)):
        print("# Preliminary list would include N exposures per cal (from nights {:s} to {:s}) ".format(night1,night2))
        print("#   bias: {:d} ".format(num_cal[band2i["zero"]]))
        for band in ["u","g","r","i","z","Y","VR"]:
            print("# {:1s}-flat: {:d} ".format(band,num_cal[band2i[band]]))

#
#   If only a count was requested then we are finished and can exit now
#
    if (args.count_only):
        exit(0)

    for iter in [0,1,2,3]:
        for index in [band2i["zero"],0,1,2,3,4,5,6]:
#           print index,max_comb,num_cal[index]
            if (int(num_cal[index]) > max_comb):
                ncut=int(num_cal[index])-max_comb
                if (iter == 0):
                    ndcut=int(2*int(num_cal[index])/ncut)+1
                elif (iter == 1):
                    ndcut=int(1.5*int(num_cal[index])/ncut)+1
                elif (iter > 1):
                    ndcut=int(int(num_cal[index])/ncut)+1

#               print("# index({:d}): {:4d} {:4d} {:4d} ".format(index,num_cal[index],ncut,ndcut))
                icnt=0
                icut=0
                new_record=[]
                for exp_rec in exp_record:
                    if (index == band2i["zero"]):
                        if ((exp_rec["obstype"]=="zero")and(exp_rec["seqid"]>0)):
                            icnt=icnt+1
                            if (icnt%ndcut == 0):
                                exp_rec["seqid"]=-1
                                icut=icut+1
                    elif (index < 7):
                        if ((exp_rec["obstype"]=="dome flat")and(band2i[exp_rec["band"]]==index)and(exp_rec["seqid"]>0)):
                            icnt=icnt+1
                            if (icnt%ndcut == 0):
                                exp_rec["seqid"]=-1
                                icut=icut+1
                    if (exp_rec["seqid"]>0):
                        new_record.append(exp_rec)
                del exp_record
                exp_record=new_record
                del new_record

            new_record=[]
            num_cal=numpy.zeros(8,dtype=numpy.int32)
            for exp_rec in exp_record:
                iseq=exp_rec["seqid"]
                if (iseq > 0):
                    iband=-1
                    if (exp_rec["obstype"]=="dome flat"):
                        iband=band2i[exp_rec["band"]]
                    elif (exp_rec["obstype"]=="zero"):
                        iband=band2i["zero"]
                    if (iband > -1):
                        num_cal[iband]=num_cal[iband]+1
                        new_record.append(exp_rec)
            del exp_record
            exp_record=new_record
            del new_record

        if (args.verbose):
            print("# Revised list would include N exposures per cal (iter={:d})".format(iter))
            print("#   bias: {:d} ".format(num_cal[band2i["zero"]]))
            for band in ["u","g","r","i","z","Y","VR"]:
                print("# {:1s}-flat: {:d} ".format(band,num_cal[band2i[band]]))

        
    if (args.idlist):
#
#       If ID list is all that is called for then dump the appropriate set
#
        cat_idlist=''
        for cat_index,exp_rec in enumerate(exp_record):
            if (cat_index == 0):
                cat_idlist='expnum='+'%d' % exp_rec["expnum"]
            else:
                cat_idlist=cat_idlist+','+'%d' % exp_rec["expnum"]
        print("{:s}".format(cat_idlist))
    else:
#
#       Otherwise provide the "oh so nifty" summary
#
        print("# Final list includes exposures from nights {:s} to {:s} ".format(night1,night2))
        print("# ")
        print("#   exposure                                                                       exposure  since                       AIR             PROPOSAL ")
        print("#      id      File Name            date_obs              obstype       seq filter   time    last   TEL_RA    TEL_DEC   MASS  PROJECT      ID          OBJECT ")
        print("#---------------------------------------------------------------------------------------------------------------------------------------------------------------- ")
        for exp_rec in exp_record:
#
#           line by line summary of exposure information
#
#           if (exp_rec["seqid"]>0):
#            print exp_rec["expnum"],exp_rec["filename"],exp_rec["date_obs"][0:21],exp_rec["obstype"],exp_rec["seqid"],exp_rec["band"],
#            exp_rec["exptime"],exp_rec["elapsed"],exp_rec["ra"],exp_rec["dec"],exp_rec["airmass"],
#            exp_rec["project"],exp_rec["propid"],exp_rec["object"]

            print(" {:10d} {:22s} {:21s} {:12s} {:5d} {:6s} {:6.1f} {:7.1f} {:9.5f} {:9.5f} {:5.2f} {:9s}  {:10s} {:s} ".format(
            exp_rec["expnum"],exp_rec["filename"],exp_rec["date_obs"][0:21],exp_rec["obstype"],exp_rec["seqid"],exp_rec["band"],
            exp_rec["exptime"],exp_rec["elapsed"],exp_rec["ra"],exp_rec["dec"],exp_rec["airmass"],
            exp_rec["project"],exp_rec["propid"],exp_rec["object"]))





