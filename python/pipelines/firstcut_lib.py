#! /usr/bin/env python

from os import path
from sys import exit
from datetime import timedelta,datetime
from despydb import DesDbi

def append_to_submitfile(args,submitfile):
    # Add determined nite,precal values to end of array.
    extraincludes = []
    for a in args:
        if a == 'section':
            continue
        else:
            line = "%s = %s" % (a,args[a])
            extraincludes.append(line)

    # Determine which block to run: fringe bands vs. non-fringe bands.
    nofringebands = ['u','g','r','i','VR']
    fringebands = ['z','Y']
    if args['band'] in fringebands:
        label = 'label = %s-fringe' % args['nite']
        block = "blocklist = se_fringe"
        extraincludes.append(label)
        extraincludes.append(block)
        fa = open(submitfile,'a')
        for item in extraincludes:
            line = "%s\n" % item
            fa.write(line)
        fa.close()
    elif args['band'] in nofringebands:
        label = 'label = %s-nofringe' % args['nite']
        block = "blocklist = se_nofringe"
        extraincludes.append(label)
        extraincludes.append(block)
        fa = open(submitfile,'a')
        for item in extraincludes:
            line = "%s\n" % item
            fa.write(line)
        fa.close()

def make_comment(date,nite,reqnum,campaign):
    comment = """
              Autosubmit started at %s
              -----
              h3. Nite: %s
              -----
              * Reqnum: %s
              * Attnum: 01

              h5. Run Status:

              h6. Merged:
              h6. Tagged:
              h6. Datastate:

              h5. Location:  /archive_data/desarchive/OPS/firstcut/%s/%s-r%s/D00*/p01
              """ % (date,nite,reqnum,campaign,nite,reqnum)
    return comment

class Get_Firstcut_Inputs():
    """Class defines functions involved with getting information for firstcut processing. Takes despydb connection cursor"""
    
    def __init__(self,section):
        """ Connect to database using user's .desservices file"""
        dbh = DesDbi(None,section)
        self.section = section        
        self.cur = dbh.cursor()    
    
    def check_submitted(self,expnum,reqnum):
        was_submitted = "select count(*) from pfw_attempt where unitname= 'D00%s' and reqnum = '%s'" % (expnum,reqnum)
        self.cur.execute(was_submitted)
        submitted_or_not = self.cur.fetchone()[0]
        return submitted_or_not       
    
    def get_max(self):
        """Get nite of max object"""
        max_object = "select max(expnum) from exposure where obstype='object' and propid='2012B-0001' and program in ('supernova','survey','photom-std-field')"
        self.cur.execute(max_object)
        max_expnum = self.cur.fetchone()[0]
        fetch_nite = "select distinct nite from exposure where expnum=%s" % (max_expnum)
        self.cur.execute(fetch_nite)
        object_nite = self.cur.fetchone()[0]
        return max_expnum,object_nite

    def find_precal(self,date,threshold,**kwargs):
        """kwargs = [override = True or False,tag = 'precal tag']"""
        override = kwargs.get('override')
        tag = kwargs.get('tag')
        nitestring = "%s-%s-%s" % (date[:4],date[4:6],date[6:])
        nite = datetime.strptime(nitestring,"%Y-%m-%d")
        days=1
        while days <= threshold:
            find_precal = "select distinct unitname,reqnum,attnum from pfw_attempt where unitname='%s'" % str((nite - timedelta(days=days)).date()).replace('-','')
            self.cur.execute(find_precal)
            results = self.cur.fetchall()
            max = len(results) - 1
            if len(results) != 0:
                precal_unitname,precal_reqnum,precal_attnum = results[max][0],results[max][1],results[max][2]
                status_precal = "select distinct status from task where id in (select distinct task_id from pfw_attempt where unitname='%s' and reqnum=%s and attnum=%s)" % (precal_unitname,precal_reqnum,precal_attnum)
                self.cur.execute(status_precal)
                status = self.cur.fetchone()[0] 
                break
            elif len(results) == 0 or status == 1 or status is None:
                days +=1
            if days > threshold:
                break
        if days > threshold:
            if override is True:
                if tag is None:
                    print "Must specify tag if override is True!"
                    exit(0)
                else:
                    max_tagged = "select distinct unitname,reqnum, attnum from ops_proctag where tag = '%s' and unitname in (select max(unitname) from ops_proctag where tag = '%s' and unitname < %s)" % (tag,tag,date)
                    self.cur.execute(max_tagged)
                    last_results = self.cur.fetchall()
                    try:
                        precal_unitname,precal_reqnum,precal_attnum = last_results[0][0],last_results[0][1],last_results[0][2]
                    except:
                        print "No tagged precals found. Please check tag or database section used..."
                        exit(0)
            elif override is False or override is None:
                if results is None:
                    print "No precals found. Please manually select input precal..."
                    exit(0)
        precal_nite = precal_unitname
        precal_run = 'r%sp0%s' % (precal_reqnum,precal_attnum)
        return precal_nite, precal_run
    
    def get_expnums(self,nite):
        """ Get exposure numbers and band for incoming exposures"""
        print "selecting exposures to submit..."
        get_expnum_and_band = "select distinct expnum, band from exposure where nite = '%s' and propid='2012B-0001' and object not like '%%pointing%%' and object not like '%%focus%%' and object not like '%%donut%%' and object not like '%%test%%' and object not like '%%junk%%' and obstype='object' and program in ('survey','supernova','photom-std-field')" % nite
        self.cur.execute(get_expnum_and_band)
        results = self.cur.fetchall()

        return results
