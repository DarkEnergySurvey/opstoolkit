#! /usr/bin/env python
"""
Query a night (or range of nights) to determine the SN sequences present
and output a list suitable for mass-submits.

Currently sequences are always reset to 1 (not elegant as it relies on --exclude
"""

def reset_sequencer(sequencer):
    """ Reset the sequence tracker """
    sequencer={"SN-E1_g":0,"SN-E1_r":0,"SN-E1_i":0,"SN-E1_z":0,
               "SN-E2_g":0,"SN-E2_r":0,"SN-E2_i":0,"SN-E2_z":0,
               "SN-S1_g":0,"SN-S1_r":0,"SN-S1_i":0,"SN-S1_z":0,
               "SN-S2_g":0,"SN-S2_r":0,"SN-S2_i":0,"SN-S2_z":0,
               "SN-C1_g":0,"SN-C1_r":0,"SN-C1_i":0,"SN-C1_z":0,
               "SN-C2_g":0,"SN-C2_r":0,"SN-C2_i":0,"SN-C2_z":0,
               "SN-C3_g":0,"SN-C3_r":0,"SN-C3_i":0,"SN-C3_z":0,
               "SN-X1_g":0,"SN-X1_r":0,"SN-X1_i":0,"SN-X1_z":0,
               "SN-X2_g":0,"SN-X2_r":0,"SN-X2_i":0,"SN-X2_z":0,
               "SN-X3_g":0,"SN-X3_r":0,"SN-X3_i":0,"SN-X3_z":0}
    return sequencer


if __name__ == "__main__":

    import argparse
    import os
    from despydb import DesDbi 
    from opstoolkit import jiracmd
    import stat
    import time
    import csv
    import sys
    import datetime
    import numpy

    parser = argparse.ArgumentParser(description='Produce listing(s) of supernova sets (for submit) from a night range')
    parser.add_argument('--first',   action='store', type=str, default=None, help='First night (nite) to consider')
    parser.add_argument('--last',    action='store', type=str, default=None, help='Last night (nite) to consider')
    parser.add_argument('--night',   action='store', type=str, default=None, help='Night (nite) to consider')
    parser.add_argument('--fileout', action='store', type=str, default=None, help='Summary listing filename (default is STDOUT)')
    parser.add_argument('--exclude', action='store', type=str, default=None, help='List of exposures to exclude from output')
    parser.add_argument('--count',   action='store_true', default=False, help='Flag to ONLY give a rough sum of number of night-field-band found in the given night range.')
    parser.add_argument('--jira',    action='store', type=str, default=None, help='Assign to or create sub-tickets under a JIRA ticket')
    parser.add_argument('--only_exist', action='store_true', default=False, help='Flag to only use existing sub-tickets')
    parser.add_argument('--create',  action='store_true', default=False, help='Flag must be set for to enable new ticket creation (DO NOT SET for dryrun/debug)')
    parser.add_argument('--assignee', action='store', type=str, default='mjohns44', help='Assignee to use for new sub-tickets (default=mjohns44)')
#    parser.add_argument('--multi',   action='store_true', default=False, help='Flag to separate single and multi-frame sets into different output files')
#    parser.add_argument('-t', '--terse',   action='store_true', default=False, help='Flag to give terse summary listing (no individual file information)')
    parser.add_argument('-q', '--quiet',   action='store_true', default=False, help='Flag to run as silently as possible')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section', action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

    if ((args.night is None)and(args.first is None)):
        print "Must specify --night or --first for query"
        exit(1)
    if (args.night is not None):
        f_night=args.night
        l_night=args.night
    else:
        f_night=args.first
        if (args.last is None):
            l_night=args.first
        else:
            l_night=args.last
#    nite=args.night

    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

    exclude_list=[]
    if (args.exclude is not None):
        if os.path.isfile(args.exclude):
            f1=open(args.exclude,'r')
            for line in f1:
                line=line.strip()
                columns=line.split()
                if (columns[0] != "#"):
                    for expnum in range(int(columns[4]),(int(columns[5])+1)):
                        exclude_list.append(expnum)
            f1.close()
        else:
            print("Warning: Exclude file {:s} not found!  Exclude list ignored".format(args.exclude_file))

#
#   Define initial parameters needed below
#       Define a dictionary to translate filter/band to an ordinate 
#       List of bands that are allowed in output
#       Lists of shallow and deep field names
#       Sequencer (space to track individual observing sequences in a night)
#       Minimum exposure time for an observation to be considered as part of the SN survey (eliminates pointing observtions).
#
    band2i={"u":0,"g":1,"r":2,"i":3,"z":4,"Y":5}
#    band_allow=["u","g","r","i","z","Y"]
    band_allow=["g","r","i","z"]
    field_shallow=["SN-C1","SN-C2","SN-E1","SN-E2","SN-S1","SN-S2","SN-X1","SN-X2"]
    field_deep=["SN-C3","SN-X3"]
    sequencer={}
    sequencer=reset_sequencer(sequencer)
    min_exptime=29.9

#
#   Setup DB connection
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = DesDbi(desdmfile,args.section)
    cur = dbh.cursor()
#
#   If an association with a JIRA ticket has been requested then make sure that a connection can be made to jira also.
#   NOTE: Could use try for inital connect if suitable means to catch an error was known....
#
    if (args.jira is not None):
        jira_connect = jiracmd.Jira('jira-desdm')
        try:
            check_parent_issue = jira_connect.jira.issue(args.jira)
        except:
            print 'Parent issue %s does not exist!' % args.jira
            sys.exit()

#
#   Main (workhorse) query to collect exposure information
#
    queryitems = ["e.filename", "e.expnum", "e.nite", "e.date_obs", "e.time_obs", "e.mjd_obs", "e.obstype", "e.band", "e.object", "e.exptime", "e.propid", "e.program", "e.field" ]
    querylist = ",".join(queryitems)
    coldict={}
    for index, item in enumerate(queryitems):
         coldict[item]=index

    query = """select %s from %sexposure e where e.nite >= '%s' and e.nite <= '%s' order by e.mjd_obs  """ % ( querylist, db_Schema, f_night, l_night )
    
    if args.verbose:
        print query
    cur.arraysize = 1000 # get 1000 at a time when fetching
    cur.execute(query)

#
#   Parse database output
#
    exp_list=[]
    night_current=0
    field_current="A"
    band_current="A"
    last_mjd=-1.
    last_expnum=-1
    for item in cur:
        if ((item[coldict["e.program"]]=="supernova")and(item[coldict["e.exptime"]] > min_exptime)):
            tmp_expdict={}
            tmp_expdict["expnum"]=int(item[coldict["e.expnum"]])
            tmp_expdict["filename"]=item[coldict["e.filename"]]
            tmp_expdict["nite"]=int(item[coldict["e.nite"]])
#
#           If the night has changed then internal counters need to be reset
#
            if (tmp_expdict["nite"] != night_current):
                night_current=tmp_expdict["nite"]
                sequencer=reset_sequencer(sequencer)
                field_current="A"            
                band_current="A"
                last_expnum=-1
                last_mjd=-1

            tmp_expdict["dateobs"]=item[coldict["e.date_obs"]]
            tmp_expdict["obstype"]=item[coldict["e.obstype"]]
            if (item[coldict["e.band"]] is None):
                tmp_expdict["band"]='NA'
            else:
                tmp_expdict["band"]=item[coldict["e.band"]]
            tmp_expdict["exptime"]=float(item[coldict["e.exptime"]])
            tmp_expdict["propid"]=item[coldict["e.propid"]]
            tmp_expdict["object"]=item[coldict["e.object"]]
            tmp_expdict["field"]=item[coldict["e.field"]]
            tmp_expdict["mjd_obs"]=float(item[coldict["e.mjd_obs"]])
            if (last_mjd == -1):
                tmp_expdict["elast"]=0.0
            else:
                tmp_expdict["elast"]=3600.*24.*(tmp_expdict["mjd_obs"]-last_mjd)

#
#           Attempt to reconicle observation as part of sequence for a field band pair.
#           First check that the name makes sense.
#
            fband='%s_%s' % (tmp_expdict["field"],tmp_expdict["band"])
            if (fband not in sequencer):
                print "Warning: Field-band not in sequencer: ",fband," for expnum=",tmp_expdict["expnum"]

            if (tmp_expdict["field"] in field_shallow):
#
#               Field is a shallow field (update sequence ID for all bands if a gap in set is found)
#
                if ((tmp_expdict["field"]!=field_current)or(tmp_expdict["expnum"]!=(last_expnum+1))):
                    for band in band_allow:
                        fband='%s_%s' % (tmp_expdict["field"],band)
                        sequencer[fband]=sequencer[fband]+1
                    fband='%s_%s' % (tmp_expdict["field"],tmp_expdict["band"])

            elif (tmp_expdict["field"] in field_deep):
#
#               Field is a deep field... update sequence ID for single band... if a gap or change of band occurs update appropriately
#
                if (((tmp_expdict["field"]!=field_current)and(tmp_expdict["band"]!=band_current))or(tmp_expdict["expnum"]!=(last_expnum+1))):
                    sequencer[fband]=sequencer[fband]+1
            else:
#
#               Doh!
#
                print "Warning: Unrecognized SN field: ",tmp_expdict["field"]," for expnum=",tmp_expdict["expnum"]              

#
#           Assign sequence ID to observation.
#           Add/append record into working list.
#           Update counters needed for next exposure
#            
            tmp_expdict["seq"]=sequencer[fband]
            exp_list.append(tmp_expdict)

            last_mjd=tmp_expdict["mjd_obs"]
            last_expnum=tmp_expdict["expnum"]
            field_current=tmp_expdict["field"]
            band_current=tmp_expdict["band"]

    new_exp_list=[]
    for erec in exp_list:
        if (erec["seq"]!=1):
            print "#Odd sequencing: ",erec["nite"],erec["expnum"],erec["field"],erec["band"],erec["seq"],erec["exptime"],erec["object"]
            erec["seq"]=1
        new_exp_list.append(erec)
    exp_list=new_exp_list

#
#   Since we have made it this far the query probably succeeded.  
#   Go ahead and open the output file if that was requested.
#
    if (args.fileout is None):
        sys.stdout.flush()
        fout=sys.stdout
    else:
        fout=open(args.fileout,"w")
 
#
#   Go through the list of exposures and break into groups for each nite.
#       Eliminate exposures that were in an non-standard band or that were in the exclude_lists
#       Also use result to build a list of nights that will be present in the output.
#
    nite_list=[]
    sn_sets={}
    for erec in exp_list:
        if ((erec["band"] in band_allow)and(erec["exptime"]>min_exptime)and(erec["expnum"]not in exclude_list)):
            if (erec["nite"] not in sn_sets):
                sn_sets[erec["nite"]]={}
                nite_list.append(erec["nite"])
            fband='%s_%s_s%d' % (erec["field"],erec["band"],erec["seq"])
            if (fband not in sn_sets[erec["nite"]]): 
                sn_sets[erec["nite"]][fband]={}
                sn_sets[erec["nite"]][fband]["field"]=erec["field"]
                sn_sets[erec["nite"]][fband]["band"]=erec["band"]
                sn_sets[erec["nite"]][fband]["seq"]=erec["seq"]
                sn_sets[erec["nite"]][fband]["expnum"]=[]
            sn_sets[erec["nite"]][fband]["expnum"].append(erec["expnum"])

#
#   Remove duplicates and sort list of nights.
#
    nite_list=sorted(list(set(nite_list)))

#
#   If option --count is used then all that is wanted is a quick sum over the number of submits that will be generated.
#   Do this... then close up shop.
#
    if (args.count):
        shallow_submit=0
        deep_submit=0
        for nite in nite_list:
            set_list=[]
            for fkey in sn_sets[nite]:
                set_list.append(fkey)
            set_list=sorted(list(set(set_list)))
            for fkey in set_list:
                if (sn_sets[nite][fkey]["field"] in field_shallow):
                    shallow_submit=shallow_submit+1
                else:
                    deep_submit=deep_submit+1
        fout.write(" Identified {:d} submits (shallow:deep={:d}:{:d})\n".format(int(shallow_submit+deep_submit),int(shallow_submit),int(deep_submit)))
        if (fout is not sys.stdout):
            fout.close()
        sys.stdout.flush()
        exit(0)

#
#   Obtain a list of JIRA IDs for each nite (to be used for REQNUMs)
#       otherwise fill in a filler value for the ticket name.
#
    reqnum_ticket={}
    if (args.jira is None):
        for nite in nite_list:
            reqnum_ticket[nite]='REQNUM'
    else:
        for nite in nite_list:
            subissue_exists = jira_connect.search_for_issue(args.jira,nite)
            if (subissue_exists[1] != 0):
                if (args.verbose):
                    print "#JIRA ticket exists for %s. Will use %s." % (nite,subissue_exists[0][0].key)
                reqnum = str(subissue_exists[0][0].key).split('-')[1]
                reqnum_ticket[nite]=reqnum
            else:
#
#               Prepare ticket items for description
#
                set_list=[]
                for fkey in sn_sets[nite]:
                    set_list.append(fkey)
                set_list=sorted(list(set(set_list)))
                set_list_items=",".join(set_list)
                description = "known unitname(s): %s" % ( set_list_items )

                if (not(args.create))or(args.only_exist):
                    if (args.verbose):
                        print "#JIRA ticket does not exist.  Using placeholder."
                    reqnum_ticket[nite]='REQNUM'
#
#                   Tell the people what they are missing.
#
                    if (args.verbose):
                        print "#JIRA ticket did not exist for nite %s under parent %s " % (nite,args.jira)
                        print "# Hypothetical ticket had:"
                        print "#     summary: %s " % nite
                        print "#   issuetype:{'name':'Sub-task'}"
                        print "# description: %s " % description
                        print "#      parent: {'key': %s}" % args.jira
                        print "#    assignee: {'name': %s}" % args.assignee

                else:
                    if (args.verbose):
                        print "#JIRA ticket does not exist. Creating one..."
#
#                   Following line should replace the one below it for tests when you a worried about committing to JIRA...
#
#                    subticket = str(jira_connect.create_jira_subtask('GROLSCH',nite,description,args.assignee))
                    subticket = str(jira_connect.create_jira_subtask(args.jira,str(nite),description,args.assignee))
                    reqnum = subticket.split('-')[1]
                    reqnum_ticket[nite]=reqnum
#    print reqnum_ticket 

# 
#   Write out the results (in a format suitable for mass-submit):.
#

    fout.write("#{:12s} {:8s} {:5s} {:2s} {:3s} {:6s} {:10s} {:10s} {:10s}\n".format('      ','      ','     ','Ba','Seq',' First',' Exp ',' Seq  ',' '))
    fout.write("#{:12s} {:8s} {:5s} {:2s} {:3s} {:6s} {:10s} {:10s} {:10s}\n".format('REQNUM','  NITE','Field','nd','NUM','Expnum',' Type',' Type ','Expnum(s)'))
    for nite in nite_list:
        set_list=[]
        for fkey in sn_sets[nite]:
            set_list.append(fkey)
        set_list=sorted(list(set(set_list)))
        for fkey in set_list:
            if (sn_sets[nite][fkey]["band"] in ['z','Y']):
                exptype='fringe'
            else:
                exptype='nofringe'
            if (len(sn_sets[nite][fkey]["expnum"])>1):
                ccdtype='combined'
            else:
                ccdtype='single'
            explist=",".join(str(x) for x in sn_sets[nite][fkey]["expnum"])
#            fout.write("REQNUM {:8s} {:5s} ".format(nite,sn_sets[nite][fkey]["field"],sn_sets[nite][fkey]["band"]," 1 ",sn_sets[nite][fkey]["expnum"][0],exptype,ccdtype,explist
            fout.write(" {:12s} {:8d} {:5s} {:2s} {:2d} {:7d} {:10s} {:10s} {:s}\n".format(reqnum_ticket[nite],nite,sn_sets[nite][fkey]["field"],sn_sets[nite][fkey]["band"],sn_sets[nite][fkey]["seq"],sn_sets[nite][fkey]["expnum"][0],exptype,ccdtype,explist))

#
#   Close up shop.
#            
    if (fout is not sys.stdout):
        fout.close()
    sys.stdout.flush()

    exit(0)
      
