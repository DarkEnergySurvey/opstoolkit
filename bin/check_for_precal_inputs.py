#! /usr/bin/env python3
"""
Query a night to determine the calibration (bias/flat) observations present.
"""

if __name__ == "__main__":

    import argparse
    import os
    import despydb.desdbi 
#    import stat
#    import time
#    import csv
#    import sys
#    import datetime
#    import numpy

#
#   Note, current possible additions/enhancements might be:
#       - to provide a default for --night to probe the current night
#       - to add the --nogaps functionality to ensure sequences without holes have arrived.
#
#
    parser = argparse.ArgumentParser(description='Check whether sufficient calibrations have arrived for PRECAL to run')
    parser.add_argument('-n', '--night',   action='store', type=str, default=None, help='Night (nite) for which to produce manifest')
    parser.add_argument('-b', '--band',    action='store', type=str, default='g,r,i,z,Y', help='Domeflat bands required to be present (default=g,r,i,z,Y)')
    parser.add_argument('--CalReq',  action='store', type=str, default=None, help='Calibration Sequence Definition nzero,nflat(band1),nflat(band2),... (NOTE: number of bands match --band)')
    parser.add_argument('--minseq',  action='store', type=str, default=None, help='Minimum number of exposure for a sequence to be considered usable (Default=5)')
    parser.add_argument('--maxseq',  action='store', type=str, default=None, help='Maximum number of exposure to be added per band/zero (Default=10).')
    parser.add_argument('--t_crit',  action='store', type=str, default=None, help='Overhead time (in seconds) allowed for a sequence to be contiguous (Default=60)')
#    parser.add_argument('--nogaps',  action='store_true', default=False, help='Flag adds requirement that sequences have no gaps (that --minseq consecutive exposures are present')
    parser.add_argument('--terse',   action='store_true', default=False, help='Flag to give terse summary listing (no individual file information)')
    parser.add_argument('--quiet',   action='store_true', default=False, help='Flag to run as silently as possible')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section', action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)

#
#   Define a dictionary to translate filter/band to an ordinate 
#   Setup the initial structure that will keep track of number of calibrations.
#
    band2i={"u":0,"g":1,"r":2,"i":3,"z":4,"Y":5,"VR":6}
    CalReq=[0,[0 for k in band2i]]

    if (args.night is None):
        print("Must specify night (-n) for query")
        exit(1)
    nite=args.night

    if (args.t_crit is None):
        t_crit=60.0
    else:
        t_crit=float(args.t_crit)

    if (args.minseq is None):
        MinSeq=5
    else:
        MinSeq=int(args.minseq)
    if (not(args.terse)):
        print( "MIN SEQ: ",MinSeq)
 
    if (args.maxseq is None):
        MaxSeq=10
    else:
        MaxSeq=int(args.maxseq)
    if (not(args.terse)):
        print("MAX SEQ: ",MaxSeq)
 
    #ReqBand=list(args.band)
    ReqBand = args.band.split(',')
    OddBandOut=False
    for band in ReqBand:
        if (band not in band2i):
            print("Reference to unknown band {:s} in --band".format(band))
            OddBandOut=True
    if (OddBandOut):
        print("Aborting")
        exit(0)
    if (not(args.terse)):
        print("Req Bands: ",ReqBand)

    if (args.CalReq is None):
        CalReq[0]=10
        for band in ReqBand:
            CalReq[1][band2i[band]]=10
    else:
        tmp_CalReq=args.CalReq.split(",")     
        if (len(tmp_CalReq)-1 != len(ReqBand)):
            print("Inconsistant number of zeros/flats (1+{:d}) in --CalReq and number of required bands ({:d}) in --band".format(len(tmp_CalReq)-1,len(ReqBand)))
            print("Aborting!")
            exit(0)
        CalReq[0]=int(tmp_CalReq[0])
        for i, bcnt in enumerate(tmp_CalReq):
            if (i>0):
                CalReq[1][band2i[ReqBand[i-1]]]=int(bcnt)
    if (not(args.terse)):
        print("Cal Req[bias]: {:d}".format(CalReq[0]))
        for band in ReqBand:
            print("Cal Req[{:s}-flat]: {:}".format(band,CalReq[1][band2i[band]]))

    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

#
#   Setup database connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
    cur = dbh.cursor()

#
#   Main (workhorse) query to collect exposure information
#
    queryitems = ["e.filename", "e.expnum", "e.nite", "e.date_obs", "e.time_obs", "e.mjd_obs", "e.obstype", "e.band", "e.object", "e.exptime" ]
    querylist = ",".join(queryitems)
    coldict={}
    for index, item in enumerate(queryitems):
         coldict[item]=index

    query = """select %s from %sexposure e where e.nite='%s' and (e.obstype='dome flat' or e.obstype='zero') order by e.mjd_obs  """ % ( querylist, db_Schema, nite )
    if args.verbose:
        print(query)
    cur.arraysize = 1000 # get 1000 at a time when fetching
    cur.execute(query)

    exp_list=[]
    exp_dict={}
    
    num_zero=0
    num_dflat=[0]*len(band2i)
    num_seq={}
    nfsum=0
    othersum=0

    lastmjd=-1.
    last_seq=0
    last_etime=0
    last_type=""
    last_band=""

    for item in cur:
        #print item
        expnum=int(item[coldict["e.expnum"]])
        exp_list.append(expnum)
        tmp_exprec={}
        tmp_exprec["expnum"]=expnum
        tmp_exprec["filename"]=item[coldict["e.filename"]]
        tmp_exprec["dateobs"]=item[coldict["e.date_obs"]]
        tmp_exprec["obstype"]=item[coldict["e.obstype"]]
        tmp_exprec["object"]=item[coldict["e.object"]]
        if (item[coldict["e.band"]] is None):
            tmp_exprec["band"]='NA'
        else:
            tmp_exprec["band"]=item[coldict["e.band"]]
#
#       Initial sum for numbers of exposures available
#
        if (tmp_exprec["obstype"] == "zero"):
            num_zero=num_zero+1
        elif (tmp_exprec["obstype"] == "dome flat"):
            nfsum=nfsum+1
            if (tmp_exprec["band"] in ReqBand):
                num_dflat[band2i[tmp_exprec["band"]]]=num_dflat[band2i[tmp_exprec["band"]]]+1
        else:
            othersum=othersum+1
#
#       Assess timing between exposures and sequence information
#
        tmp_exprec["exptime"]=float(item[coldict["e.exptime"]])
        tmp_exprec["mjd_obs"]=float(item[coldict["e.mjd_obs"]])
        if (lastmjd == -1):
            tmp_exprec["elapsed"]=0.0
        else:
            tmp_exprec["elapsed"]=3600.*24.*(tmp_exprec["mjd_obs"]-lastmjd)

#
#       Rules for whether a new sequence has been detected
#       Assign exposure into sequence
#
        IncrementSequence=False
        if (tmp_exprec["elapsed"]>(last_etime+t_crit)):
            IncrementSequence=True
        if (tmp_exprec["obstype"]!=last_type):
            IncrementSequence=True
        if ((tmp_exprec["obstype"]=="dome flat")and(tmp_exprec["band"]!=last_band)):
            IncrementSequence=True
        if (IncrementSequence):
            last_seq=last_seq+1
            num_seq[last_seq]=0

        if (tmp_exprec["obstype"] in ['zero','dome flat']):
            if ((IncrementSequence)and(tmp_exprec["obstype"] == 'zero')):
                tmp_exprec["seqid"]=-1
            else:
                tmp_exprec["seqid"]=last_seq
                num_seq[last_seq]=num_seq[last_seq]+1
            if (tmp_exprec["band"] == "r") and (tmp_exprec["exptime"] > 15.):
                tmp_exprec["seqid"]=-1
            if (tmp_exprec["object"] is not None):
                if ("test" in tmp_exprec["object"]) or ("junk" in tmp_exprec["object"]) or ("PTC" in tmp_exprec["object"]):
                    tmp_exprec["seqid"]=-1

        else:
            tmp_exprec["seqid"]= -1
#
#       Add exposure into dictionary
#       Update variables that track the characteristics of the previous exposure (with the current exposures)
#
        exp_dict[expnum]=tmp_exprec
        lastmjd=tmp_exprec["mjd_obs"]
        last_etime=item[coldict["e.exptime"]]
        last_type=item[coldict["e.obstype"]]
        last_band=item[coldict["e.band"]]
    
    if (not(args.terse)):
        print("Number of exposures considered: {:d}".format(len(exp_list)))
        FlatReportList=[]
        for band in ReqBand:
            FlatReportList.append("%s:%d" % (band,num_dflat[band2i[band]]))
        FlatReportString=",".join(FlatReportList)
        print("Preliminary breakdown: zero={:d} flat=({:s})".format(num_zero,FlatReportString))
#
#   Remove first frame from each bias sequence
#
    for seqnum in range(1,last_seq+1):
        if (num_seq[seqnum]<MinSeq):
            for expnum in exp_list:
                if (exp_dict[expnum]["seqid"]==seqnum):
                    exp_dict[expnum]["seqid"]=-1*seqnum
            num_seq[seqnum]=0

#
#   Zero out counters and then...
#   Re-evaluate sum for numbers of exposures available
#
    num_zero=0
    num_dflat=[0]*len(band2i)
    num_seq={}
    nfsum=0
    othersum=0
    flat_list=[]
    zero_list=[]
#    print(CalReq)
    for expnum in exp_list:
#        print(expnum,exp_dict[expnum]["seqid"],exp_dict[expnum]["obstype"],exp_dict[expnum]["band"],exp_dict[expnum]['exptime'])
        if (exp_dict[expnum]["seqid"]>0):
            if (exp_dict[expnum]["obstype"] == "zero"):
                if ((num_zero < MaxSeq)or(num_zero < CalReq[0])):
                    zero_list.append(str(expnum))
                num_zero=num_zero+1
            elif (exp_dict[expnum]["obstype"] == "dome flat"):
                if ((num_dflat[band2i[exp_dict[expnum]["band"]]] < MaxSeq)or(num_dflat[band2i[exp_dict[expnum]["band"]]] < CalReq[1][band2i[exp_dict[expnum]["band"]]])):
                    flat_list.append(str(expnum))
                nfsum=nfsum+1
                if (exp_dict[expnum]["band"] in ReqBand):
                    num_dflat[band2i[exp_dict[expnum]["band"]]]=num_dflat[band2i[exp_dict[expnum]["band"]]]+1
            else:
                othersum=othersum+1
    zero_string = ",".join(zero_list)
    flat_string = ",".join(flat_list)

#
#   Revised (after removing short groups and initial biases)
#
    if (not(args.terse)):
        FlatReportList=[]
        for band in ReqBand:
            FlatReportList.append("%s:%d" % (band,num_dflat[band2i[band]]))
        FlatReportString=",".join(FlatReportList)
        print("     Final breakdown: zero={:d} flat=({:s})".format(num_zero,FlatReportString))
#
#   Determine whether criteria have been met for precal to run
#
#   Criteria 1: num_zero >= CalReq[0]
#
    LetsRock=True
    if (num_zero < CalReq[0]):
        LetsRock=False
        if (not(args.terse)):
            print("Insufficient number of zero/bias frames found.  Game Over.")
    else:
        if (not(args.terse)):
            print("Zero/bias frames found.  Check.")
#
#   Criteria 2: num_flat(band) >= CalRec[1][band2i[(band)]]
#
    for index, band in enumerate(ReqBand):
#        print index, band, num_dflat[band2i[band]], CalReq[1][index]
        if (num_dflat[band2i[band]] < CalReq[1][index]):
            LetsRock=False
            if (not(args.terse)):
                print("Insufficient number of {:s}-band flat frames found.  Game Over.".format(band))
        else:
            if (not(args.terse)):
                print("{:s}-band flat frames found.  Check.".format(band))
#
#   Criteria 3: if --nogaps then make sure sequences do not have holes/gaps (a file has not yet arrived)
#
#    if (args.nogaps):

#
#   And we have our answer.
#
    if (LetsRock):
        print("LETS ROCK!")
        print("HERE ARE YOUR LISTS:")
        print("bias_expnum={:s}".format(zero_string))
        print("dflat_expnum={:s}".format(flat_string))

#    if (exp_dict[expnum]["seqid"]>0):
#        print expnum,exp_dict[expnum]["seqid"],exp_dict[expnum]["obstype"],exp_dict[expnum]["band"]

    exit(0)

