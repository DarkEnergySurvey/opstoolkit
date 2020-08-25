#! /usr/bin/env python
"""
Query a night (or range of nights) to determine the SN sequences present
and output a list suitable for mass-submits.

Currently sequences are always reset to 1 (not elegant as it relies on --exclude
"""


####################################################################
def SetXTimeFormat(time_range,ax):
    """
    Try to set a sensible formatting for plots with a time-based coordinate 
    on the x-axis.

    time_range = range of time being plotted

    Returns:
        ax:   adjusted axis decorator/object 
        hfmt: formatter for labels
    """

    if (time_range >= 5.0):
#       Time range is many days
#       - no time (only date) in labels
#       - auto formatting seems to work well.
#       - commented code kept for now in case specification appears necessary
        hfmt = matplotlib.dates.DateFormatter('%m/%d')
        ax.xaxis.set_major_locator(matplotlib.dates.AutoDateLocator())
#        if ((time_range >=3.0)and(time_range < 8.0)):
#            ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=1))
#            ax.xaxis.set_minor_locator(matplotlib.dates.HourLocator(interval=6))
#        elif ((time_range >=8.0)and(time_range < 18.0)):
#            ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=3))
#            ax.xaxis.set_minor_locator(matplotlib.dates.DayLocator(interval=1))
#        elif ((time_range >=18.0)and(time_range < 30.0)):
#            ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=5))
#            ax.xaxis.set_minor_locator(matplotlib.dates.DayLocator(interval=1))
#        elif ((time_range >=30.0)and(time_range < 90.0)):
#            ax.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=10))
#            ax.xaxis.set_minor_locator(matplotlib.dates.DayLocator(interval=1))
#        elif ((time_range >=90.0)and(time_range < 180.0)):
#            ax.xaxis.set_major_locator(matplotlib.dates.MonthLocator(interval=1))
#            ax.xaxis.set_minor_locator(matplotlib.dates.DayLocator(interval=10))
    else:
#       A little trickier as hours are needed for meaningful tickmarks
        hfmt = matplotlib.dates.DateFormatter('%m/%d %H:%M')
        if ((time_range >=1.75)and(time_range < 5.0)):
            ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=12))
            ax.xaxis.set_minor_locator(matplotlib.dates.HourLocator(interval=2))
        elif ((time_range >= 18./24.)and(time_range < 1.75)):
            ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=6))
            ax.xaxis.set_minor_locator(matplotlib.dates.HourLocator(interval=1))
        elif ((time_range >= 8./24.)and(time_range < 18./24.)):
            ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=3))
            ax.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(interval=30))
        elif ((time_range >= 3./24.)and(time_range <  8./24.)):
            ax.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=1))
            ax.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(interval=10))
        elif ((time_range >= 1.6/24.)and(time_range <  3./24.)):
            ax.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(interval=30))
            ax.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(interval=5))
        elif ((time_range >= 1.1/24.)and(time_range <  1.6/24.)):
            ax.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(interval=20))
            ax.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(interval=5))
        elif ((time_range >= 1.1/24.)):
            ax.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(interval=10))
            ax.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(interval=2))

    ax.xaxis.set_major_formatter(hfmt)

    return ax

####################################################################
def qaplot_job_throughput(data,PlotFname):
    """
    Produce QA plot show job throughput history.

    data      = dictionary holding vectors that will be plotted.
                'x1': vector of submit times (in days)
                'y1': vector showing ordinal job number
                'y2': vector showing total time to execute each job
                'color': vector containing a set of tags showing each job type
    PlotFname = output filename
    """

#   If QA plots have been requested then output an empty QA plot that gives 
#   information about the failure.

#   Check to see if data for color coding were specified.
    colors=['red','green','blue','cyan','magenta','orange','grey','black']
    symbols=['.','x','o']
    font_legend={'family':'serif','weight':'normal','size':7}
    font_axis={'family':'serif','weight':'normal','size':9}
#
#   If color information is provided then discover the discreet sets
#
    c_count=0
    s_count=0
    if ('color' in data):
        data_subsets=list(set(data['color']))
        data_legend={}
        for dtype in data_subsets:
            data_legend[dtype]={}
            data_legend[dtype]['color']=colors[c_count]
            data_legend[dtype]['symbol']=symbols[s_count]
            c_count=c_count+1
            if (c_count >= len(colors)):
                c_count=0
                s_count=s_count+1

#
#   Convert datetime to 
#
    x1num=matplotlib.dates.date2num(data['x1'])

#
#   Find min/max for data (for setting limits)
#

    winx1_min=min(x1num)
    winx1_max=max(x1num)
    winx1_range=winx1_max-winx1_min
#    winx1_max=max(data['x1'])
    winy1_min=0.0
    winy1_max=max(data['y1'])
    winy2_max=max(data['y2'])

    print winx1_min,winx1_max,(winx1_max-winx1_min)

#
#   Get ready to plot
#
#    plt.figure(figsize=(12,6),dpi=300)
    plt.figure(figsize=(12,6),dpi=300)
#    plt.subplot(2,1,1)
    ax=plt.subplot(3,1,1)

    if ('color' in data):
        xpos_point=winx1_min+0.025*(winx1_range)
        xpos_label=winx1_min+0.035*(winx1_range)
        ypos_point=winy1_max-(0.025*(winy1_max-winy1_min))
        ypos_step=0.05*(winy1_max-winy1_min)
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['color']):
                if (entry == dtype):
                    xtmp.append(x1num[index])
                    ytmp.append(data['y1'][index])
            if (len(xtmp)>0):
                plt.scatter(xtmp,ytmp,marker='.',color=data_legend[dtype]['color'])
            ypos_point=ypos_point-ypos_step
            plt.plot(xpos_point, ypos_point, marker=data_legend[dtype]['symbol'], label=dtype, color=data_legend[dtype]['color'])
            plt.text(xpos_label, (ypos_point-0.25*ypos_step), ('%s' % dtype),fontdict=font_legend)
    else:
#        plt.scatter(data['x1'],data['y1'],marker='.',color='blue')
        plt.scatter(x1num,data['y1'],marker='.',color='blue')

#
#   Try to choose a sane formatting for the data range in plot.
#
    ax = SetXTimeFormat(winx1_range,ax)
#    plt.axis([winx1_min,winx1_max,0.,winy1_max],fontdict=font_axis)
    plt.axis()

#    plt.xlabel('Start time [days]')
    plt.ylabel('Number of runs')

    ax=plt.subplot(3,1,2)
    if ('color' in data):
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['color']):
                if (entry == dtype):
                    xtmp.append(x1num[index])
#                    xtmp.append(data['x1'][index])
                    ytmp.append(data['y2'][index])
            if (len(xtmp)>0):
                plt.scatter(xtmp,ytmp,marker=data_legend[dtype]['symbol'],color=data_legend[dtype]['color'])
    else:
        plt.scatter(x1num,data['y2'],marker='.',color='blue')

#
#   Try to choose a sane formatting for the data range in plots.
#
    ax = SetXTimeFormat(winx1_range,ax)
    plt.axis([winx1_min,winx1_max,0.,winy2_max],fontdict=font_axis)
    plt.xlabel('Start time')
    plt.ylabel('Processing time')

    plt.savefig("%s" % (PlotFname))
    exit_code=0

    return exit_code

####################################################################
def qaplot_job_throughput2(data,PlotFname):
    """
    Produce QA plot show job throughput history.

    data      = dictionary holding vectors that will be plotted.
                'x1': vector of times bins (in days)
                'y1': dictionary holding histogram data running
                'color': vector containing a set of tags showing each job type
    PlotFname = output filename
    """

#   If QA plots have been requested then output an empty QA plot that gives 
#   information about the failure.

#   Check to see if data for color coding were specified.
    colors=['red','green','blue','cyan','magenta','orange','grey','black']
    symbols=['.','x','o']
    font_legend={'family':'serif','weight':'normal','size':7}
    font_axis={'family':'serif','weight':'normal','size':9}
#
#   If color information is provided then discover the discreet sets
#
    RunningStacked=[]
    StartedStacked=[]
    SuccessStacked=[]
    FailureStacked=[]

    c_count=0
    s_count=0
    data_legend={}
    data_subsets=[]
    data_color=[]
    for Pipeline in data['y1']:
        if (Pipeline not in data_legend):
            data_legend[Pipeline]={}
            data_legend[Pipeline]['color']=colors[c_count]
            data_legend[Pipeline]['symbol']=symbols[s_count]
            data_subsets.append(Pipeline)
            data_color.append(colors[c_count])
            c_count=c_count+1
            if (c_count >= len(colors)):
                c_count=0
                s_count=s_count+1
        RunningStacked.append(data['y1'][Pipeline]['running_list'])
        StartedStacked.append(data['y1'][Pipeline]['start_list'])
        SuccessStacked.append(data['y1'][Pipeline]['success_list'])
        FailureStacked.append(data['y1'][Pipeline]['failure_list'])
#
#   Get ready to plot
#
    plt.figure(figsize=(12,8),dpi=300)
###########################################################################
#   Begin first panel (plot stacked histogram showing runs vs. time

    ax=plt.subplot(3,1,1)

    plt.hist(RunningStacked,data['x1'],stacked=True,ec='None',color=data_color)
#
#   Obtain min/max for data (for setting limits, and for placing the legend)
#
    axrange=plt.axis()
    winx1_min=min(data['x1'])
    winx1_max=max(data['x1'])
    winx1_range=winx1_max-winx1_min
    winy1_min=0.0
    winy1_max=axrange[3]
#    print winx1_min,winx1_max,(winx1_max-winx1_min),winy1_max
    ax = SetXTimeFormat(winx1_range,ax)

    xpos_point=winx1_min+0.025*(winx1_range)
    xpos_label=winx1_min+0.035*(winx1_range)
    ypos_point=winy1_max-(0.025*(winy1_max-winy1_min))
    ypos_step=0.05*(winy1_max-winy1_min)
#
#   Insert legend
#
    for dtype in data_subsets:
        ypos_point=ypos_point-ypos_step
        plt.plot(xpos_point, ypos_point, marker=data_legend[dtype]['symbol'], label=dtype, color=data_legend[dtype]['color'])
        plt.text(xpos_label, (ypos_point-0.25*ypos_step), ('%s' % dtype),fontdict=font_legend)

    plt.axis([winx1_min,winx1_max,0.,axrange[3]])

#    plt.xlabel('Start time')
    plt.ylabel('Number of\nAttempts Running')

###########################################################################
#   Second panel (plot showing total run time)
#
    ax=plt.subplot(3,1,2)
    if ('color' in data):
#
#   If color information is provided then discover/plot as discreet sets
#
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['color']):
                if (entry == dtype):
                    xtmp.append(data['x2'][index])
                    ytmp.append(data['y2'][index])
            if (len(xtmp)>0):
                plt.scatter(xtmp,ytmp,marker=data_legend[dtype]['symbol'],color=data_legend[dtype]['color'])
    else:
        plt.scatter(data['x2'],data['y2'],marker='.',color='blue')

#
#   Try to choose a sane formatting for the data range in plots.
#   Force x-range to match that of first plot
#   If Y-range is greater than 48 hours then force plot to truncate.
#
    ax = SetXTimeFormat(winx1_range,ax)

    winy2_min=0.0
    winy2_max=max(data['y2'])
    if (winy2_max > 48.):
        winy2_max=48.0

    xpos_point=winx1_min+0.025*(winx1_range)
    xpos_label=winx1_min+0.035*(winx1_range)
    ypos_point=winy2_max-(0.025*(winy2_max-winy2_min))
    ypos_step=0.05*(winy2_max-winy2_min)
#
#   Insert legend (panel 2)
#
    for dtype in data_subsets:
        ypos_point=ypos_point-ypos_step
        plt.plot(xpos_point, ypos_point, marker=data_legend[dtype]['symbol'], label=dtype, color=data_legend[dtype]['color'])
        plt.text(xpos_label, (ypos_point-0.25*ypos_step), ('%s' % dtype),fontdict=font_legend)
#
#   Finish by setting explicit axis and labels
#
    plt.axis([winx1_min,winx1_max,0.,winy2_max],fontdict=font_axis)
#    plt.xlabel('Start time')
    plt.ylabel('Processing time')

###########################################################################
#   Third panel (plot showing total run time per time bin)
#
    ax=plt.subplot(3,1,3)
    if ('c3' in data):
#
#   If color information is provided then discover/plot as discreet sets
#
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['c3']):
                if (entry == dtype):
                    xtmp.append(data['x3'][index])
                    ytmp.append(data['y3'][index])
            if (len(xtmp)>0):
                plt.scatter(xtmp,ytmp,marker=data_legend[dtype]['symbol'],color=data_legend[dtype]['color'])
    else:
        plt.scatter(data['x3'],data['y3'],marker='.',color='blue')

#
#   Try to choose a sane formatting for the data range in plots.
#   Force x-range to match that of first plot
#   If Y-range is greater than 48 hours then force plot to truncate.
#
    ax = SetXTimeFormat(winx1_range,ax)

    winy3_min=0.0
    winy3_max=max(data['y3'])
    if (winy3_max > 48.):
        winy3_max=48.0

    xpos_point=winx1_min+0.025*(winx1_range)
    xpos_label=winx1_min+0.035*(winx1_range)
    ypos_point=winy3_max-(0.025*(winy3_max-winy3_min))
    ypos_step=0.05*(winy3_max-winy3_min)
#
#   Insert legend (panel 2)
#
    for dtype in data_subsets:
        if (dtype in data['c3']): 
            ypos_point=ypos_point-ypos_step
            plt.plot(xpos_point, ypos_point, marker=data_legend[dtype]['symbol'], label=dtype, color=data_legend[dtype]['color'])
            plt.text(xpos_label, (ypos_point-0.25*ypos_step), ('%s' % dtype),fontdict=font_legend)
#
#   Finish by setting explicit axis and labels
#
    plt.axis([winx1_min,winx1_max,0.,winy3_max],fontdict=font_axis)
    plt.xlabel('Start time')
    plt.ylabel('Median Processing\nTime per Bin')


    plt.savefig("%s" % (PlotFname))
    exit_code=0

    return exit_code



####################################################################
def qaplot_transfer1(data,PlotFname):
    """
    Produce QA plot show job throughput history.

    data      = dictionary holding vectors that will be plotted.
                'x1': vector of submit times (in days)
                'y1': vector showing total wait time in
                'y2': vector showing total wait time out
                'y3': vector showing total xfer time in
                'y4': vector showing total xfer time out
                'color': vector containing a set of tags showing each job type
    PlotFname = output filename
    """

#   If QA plots have been requested then output an empty QA plot that gives 
#   information about the failure.

#   Check to see if data for color coding were specified.
    colors=['red','green','blue','cyan','magenta','orange','grey','black']
    symbols=['.','x','o']
    font_legend={'family':'serif','weight':'normal','size':7}
#
#   If color information is provided then discover the discreet sets
#
    c_count=0
    s_count=0
    if ('color' in data):
        data_subsets=list(set(data['color']))
        data_legend={}
        for dtype in data_subsets:
            data_legend[dtype]={}
            data_legend[dtype]['color']=colors[c_count]
            data_legend[dtype]['symbol']=symbols[s_count]
            c_count=c_count+1
            if (c_count >= len(colors)):
                c_count=0
                s_count=s_count+1
#
#   Find min/max for data (for setting limits)
#
    winx1_max=max(data['x1'])
    winy1_max=max(data['y1'])
    winy2_max=max(data['y2'])
    winy3_max=max(data['y3'])
    winy4_max=max(data['y4'])

#
#   Get ready to plot
#
    plt.figure(figsize=(12,9),dpi=300)

    plt.subplot(4,1,1)

    if ('color' in data):
        xpos_point=0.025*(winx1_max-0.)
        xpos_label=0.035*(winx1_max-0.)
        ypos_point=winy1_max-(0.025*(winy1_max-0.))
#        ypos_step=0.05*(winy1_max-0.)
        ypos_step=2.0
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['color']):
                if (entry == dtype):
                    xtmp.append(data['x1'][index])
                    ytmp.append(data['y1'][index])
            if (len(xtmp)>0):
#                plt.scatter(xtmp,ytmp,marker=data_legend[dtype]['symbol'],color=data_legend[dtype]['color'])
                plt.semilogy(xtmp,ytmp,marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])

            ypos_point=ypos_point/ypos_step
            plt.plot(xpos_point, ypos_point, marker=data_legend[dtype]['symbol'], label=dtype, color=data_legend[dtype]['color'])
            plt.text(xpos_label, ypos_point, ('%s' % dtype),fontdict=font_legend)
    else:
#        plt.scatter(data['x1'],data['y1'],marker='.',color='blue')
        plt.semilogy(data['x1'],data['y1'],marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])
    plt.axis([0.,winx1_max,-3.,winy1_max])
#    plt.xlabel('Start time [days]')
    plt.ylabel(r'$\Sigma$ Wait$_{IN}$ [hr]')

    plt.subplot(4,1,2)
    if ('color' in data):
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['color']):
                if (entry == dtype):
                    xtmp.append(data['x1'][index])
                    ytmp.append(data['y2'][index])
            if (len(xtmp)>0):
#                plt.scatter(xtmp,ytmp,marker=data_legend[dtype]['symbol'],color=data_legend[dtype]['color'])
                plt.semilogy(xtmp,ytmp,marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])
    else:
#        plt.scatter(data['x1'],data['y2'],marker='.',color='blue')
        plt.semilogy(data['x1'],data['y2'],marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])
    plt.axis([0.,winx1_max,-3.,winy2_max])
#    plt.xlabel('Start time [days]')
    plt.ylabel(r'$\Sigma$ Xfer$_{IN}$ [hr]')

    plt.subplot(4,1,3)
    if ('color' in data):
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['color']):
                if (entry == dtype):
                    xtmp.append(data['x1'][index])
                    ytmp.append(data['y3'][index])
            if (len(xtmp)>0):
#                plt.scatter(xtmp,ytmp,marker=data_legend[dtype]['symbol'],color=data_legend[dtype]['color'])
                plt.semilogy(xtmp,ytmp,marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])
    else:
#        plt.scatter(data['x1'],data['y3'],marker='.',color='blue')
        plt.semilogy(data['x1'],data['y3'],marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])
    plt.axis([0.,winx1_max,-3.,winy3_max])
#    plt.xlabel('Start time [days]')
    plt.ylabel(r'$\Sigma$ Wait$_{OUT}$ [hr]')

    plt.subplot(4,1,4)
    if ('color' in data):
        for dtype in data_subsets:
            xtmp=[]
            ytmp=[]
            for index, entry in enumerate(data['color']):
                if (entry == dtype):
                    xtmp.append(data['x1'][index])
                    ytmp.append(data['y4'][index])
            if (len(xtmp)>0):
#                plt.scatter(xtmp,ytmp,marker=data_legend[dtype]['symbol'],color=data_legend[dtype]['color'])
                plt.semilogy(xtmp,ytmp,marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])
    else:
#        plt.scatter(data['x1'],data['y4'],marker='.',color='blue')
        plt.semilogy(data['x1'],data['y4'],marker=data_legend[dtype]['symbol'],ls='None',color=data_legend[dtype]['color'])
    plt.axis([0.,winx1_max,-3.,winy4_max])
#    plt.xlabel('Start time [days]')
    plt.ylabel(r'$\Sigma$ Xfer$_{OUT}$ [hr]')

    plt.savefig("%s" % (PlotFname))
    exit_code=0

    return exit_code

####################################################################


if __name__ == "__main__":

    import argparse
    import os
    from despydb import DesDbi 
#    from opstoolkit import jiracmd
    from opstoolkit import nite_strings
    import re
    import time
    import csv
    import sys
    import datetime
    import numpy
#
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt


    parser = argparse.ArgumentParser(description='Produce listing(s) of supernova sets (for submit) from a night range')
    parser.add_argument('--first',     action='store', type=str, default=None, help='Start of time range to consider (YYYYMMDD:HHMMSS)')
    parser.add_argument('--last',      action='store', type=str, default=None, help='End of time range to consider (YYYYMMDD:HHMMSS, default=Now)')
    parser.add_argument('--days_back', action='store', type=str, default=None, help='Express time range as last N days.')
    parser.add_argument('--time_bin',  action='store', type=str, default='1h', help='Binfactor for timing plots (e.g. 1d,1h,1m, default=1h)')
#    parser.add_argument('--fileout',   action='store', type=str, default=None, help='Summary listing filename (default is STDOUT)')
    parser.add_argument('--QAplot',    action='store', type=str, default=None, help='Root filename for QAplot(s)')
#    parser.add_argument('--jira',    action='store', type=str, default=None, help='Confine query to a ticket or sub-tickets under a JIRA ticket')
#    parser.add_argument('--only_parent', action='store_true', default=False, help='Flag to only search under the indicated ticket')
    parser.add_argument('--only_failed', action='store_true', default=False, help='Flag to only report on submissions that have resulted in failure')
    parser.add_argument('--only_running', action='store_true', default=False, help='Flag to only report on submissions that are currently running')
    parser.add_argument('--only_pipe',    action='store', type=str, default=None, help='Option to only show processing by a specific pipeline (contraint on PFW_ATTEMPT.SUBPIPEPROD)')
    parser.add_argument('--junk',    action='store_true', default=False, help='Flag to suppress junk runs')
    parser.add_argument('--terse',   action='store_true', default=False, help='Flag to give only terse summary for each run')
    parser.add_argument('--full',    action='store_true', default=False, help='Flag to processing summary for each module run')
    parser.add_argument('--fid_date', action='store', type=str, default=None, help='Define a fiducial date/time to compare submit times (format=YYYYMMDD:HHMMSS, default=None)')
#    parser.add_argument('-q', '--quiet',   action='store_true', default=False, help='Flag to run as silently as possible')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print extra (debug) messages to stdout')
    parser.add_argument('-s', '--section', action='store', type=str, default=None, help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',  action='store', type=str, default=None, help='DB schema (do not include \'.\').')
    args = parser.parse_args()
    if (args.verbose):
        print "Args: ",args

    if ((args.days_back is None)and(args.first is None)):
        print "Must specify --days_back or --first for query"
        exit(1)
#    if (args.jira is None):
#        print "Must specify --jira {parent} for query"
#        exit(1)
    if (args.QAplot is None):
        QAplotFlag=False
    else:
        QAplotFlag=True

    if (args.Schema is None):
        db_Schema=""
    else:
        db_Schema="%s." % (args.Schema)

    if (args.junk):
        suppress_junk=" and a.data_state!='JUNK'"
    else:
        suppress_junk=""

    if (args.only_running):
        only_running=" and t.status is null "
    else:
        only_running=""

    if (args.only_failed):
        only_failed=" and t.status != 0 "
    else:
        only_failed=""

    if (args.only_pipe is None):
        only_pipe=""
    else:
        only_pipe=" and r.pipeline='%s' " % args.only_pipe

    if (args.time_bin is None):
        TimeBin=1.0/24.0
    else:
        print args.time_bin[-1:]
        print args.time_bin[:-1]
        if (args.time_bin[-1:] == "h"):
            TimeBin=float(args.time_bin[:-1])/24.0
        elif (args.time_bin[-1:] == "d"):
            TimeBin=float(args.time_bin[:-1])
        elif (args.time_bin[-1:] == "m"):
            TimeBin=float(args.time_bin[:-1])/24.0/60.
        else:
            TimeBin=1.0/24.0
#
#   Pipeline specific information
#
    SN_Shallow=['SN-E1','SN-E2','SN-S1','SN-S2','SN-C1','SN-C2','SN-X1','SN-X2']
    SN_Deep=['SN-C3','SN-X3']
#
#   Generate the time constraint for the search
#
    if (args.days_back is None):
        if (re.search(":",args.first) is None):
            first_submit=datetime.datetime.strptime(args.first,"%Y%m%d")
        else:
            first_submit=datetime.datetime.strptime(args.first,"%Y%m%d:%H%M%S")
        if (args.last is None):
            submit_constraint="and a.SUBMITTIME > to_date('%s','YYYY-MM-DD HH24:MI:SS')" % (first_submit)
        else:
            if (re.search(":",args.last) is None):
                last_submit=datetime.datetime.strptime(args.last,"%Y%m%d")
            else:
                last_submit=datetime.datetime.strptime(args.last,"%Y%m%d:%H%M%S")
            submit_constraint="and a.SUBMITTIME between to_date('%s','YYYY-MM-DD HH24:MI:SS') and to_date('%s','YYYY-MM-DD HH24:MI:SS')" % (first_submit,last_submit)
    else:
        submit_constraint="and a.SUBMITTIME > SYSDATE-%s " % (args.days_back)

#    print first_submit,last_submit

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
#   Form a list of request numbers for the nights under the parent ticket (or only the parent ticket itself
#
#    req_list=[]
#    for key in reqnum_dict:
#        req_list.append(reqnum_dict[key])
#    req_list_constraint="a.reqnum in (%s)" % (",".join(req_list))
#
#   Main (workhorse) query to collect requests under a specific ticket
#
    queryitems = ["a.reqnum", "a.unitname", "a.attnum","a.id","a.submittime","r.pipeline","r.campaign","a.operator","a.data_state","a.archive_path","a.task_id","t.start_time","t.end_time","t.status"]
    querylist = ",".join(queryitems)
    coldict={}
    for index, item in enumerate(queryitems):
        coldict[item]=index
    query = """select {qlist:s} from {schema:s}pfw_attempt a, {schema:s}task t, {schema:s}pfw_request r 
        where r.reqnum=a.reqnum 
            and a.task_id=t.id {sconst:s} {sjunk:s} {sorun:s} {sofail:s} {sopipe:s}
        order by a.submittime """.format(
            qlist=querylist, 
            schema=db_Schema,
            sconst=submit_constraint,
            sjunk=suppress_junk,
            sorun=only_running,
            sofail=only_failed,
            sopipe=only_pipe)

    if args.verbose:
        print query
    cur.arraysize = 1000 # get 1000 at a time when fetching
    cur.execute(query)

#
#   Get the basic information on each attempt/request
#
    attempt_dict={}
    attempt_list=[]
    tfirst_submit=None;
    for item in cur:
        d = dict(zip(queryitems, item))
        if (d["a.id"] is None):
            attempt="%d:%s:%d"%(d["a.reqnum"],d["a.unitname"],d["a.attnum"])
        else:
            attempt=d["a.id"]
        attempt_dict[attempt]=d
        attempt_list.append(attempt)

#
#   Quick follow-up query to attempt to find the exec-hosts.
#
    tmp_id=[]
    for attempt in attempt_dict:
        tmp_id.append([attempt_dict[attempt]['a.id']])
    cur.execute('delete from {:s}'.format('GTT_ID'))
    # load filenames into GTT_ID table
    if (args.verbose):
        print("# Loading {:s} table for secondary queries with IDs for {:d} attempts".format('GTT_ID',len(tmp_id)))
    dbh.insert_many('GTT_ID',['ID'],tmp_id)

    queryitems = ["g.id", "t.exec_host"]
    querylist = ",".join(queryitems)
    coldict={}
    for index, item in enumerate(queryitems):
        coldict[item]=index
    query = """select {qlist:s} from {schema:s}pfw_job j, {schema:s}task t, gtt_id g
        where g.id=j.pfw_attempt_id
            and j.jobnum=1
            and j.task_id=t.id""".format(qlist=querylist,schema=db_Schema)

    if args.verbose:
        print query
    cur.arraysize = 1000 # get 1000 at a time when fetching
    cur.execute(query)

    for item in cur:
        d = dict(zip(queryitems, item))
        if (d['g.id'] in attempt_dict):
            attempt_dict[d['g.id']]['exec_host']=d['t.exec_host']
        else:
            print('Unable to identify attempt {:d} to fill in exec_host={:s}'.format(d['g.id'],d['t.exec_host']))

#
#   Fix entries for exec_host
#   1) add Unknown for cases where no values have been returned (usually job is still starting or died before getting to target side)
#   2) add Unknown where job has started (but exec_host not yet updated)
#   3) if it looks like this is coming from a FermiGrid/DEGrid node then try to separate out a shorter identifier
#
    for attempt in attempt_dict:
        if ('exec_host' not in attempt_dict[attempt]):
            attempt_dict[attempt]['exec_host']='Unknown'
        else:
            if (attempt_dict[attempt]['exec_host'] is None):
                attempt_dict[attempt]['exec_host']='Unknown'
            if (re.search("fnal",attempt_dict[attempt]['exec_host']) is not None):
                attempt_dict[attempt]['exec_host']=(attempt_dict[attempt]['exec_host'].split(".")[0]).split("-")[-1]
            if (re.search("campuscluster",attempt_dict[attempt]['exec_host']) is not None):
                attempt_dict[attempt]['exec_host']=(attempt_dict[attempt]['exec_host'].split(".")[0]).split("-")[-1]

#
#   Set Fiducial timestamp (currently either by user request or use first submit)
#   
    if (args.fid_date is not None):
        tfirst_submit=datetime.datetime.strptime(args.fid_date,"%Y%m%d:%H%M%S")
    else:
        if (len(attempt_list)>0):
            tfirst_submit=attempt_dict[attempt_list[0]]["a.submittime"]
        else:
            tfirst_submit=-1

#
#   Post process basic information acquired to get results
#       Printable timestamps
#       Cases where processing is not finished (handle NoneTypes).
#       Relative timing and duration information
#       Known pipeline variants (currently on sne)
#
    for attempt in attempt_list:
        if (attempt_dict[attempt]["t.start_time"] is None):
            attempt_dict[attempt]["start_time"]="None?"
        else:            
            attempt_dict[attempt]["start_time"]=attempt_dict[attempt]["t.start_time"].strftime("%Y%m%d:%H%M%S")

        if (attempt_dict[attempt]["a.archive_path"] is None):
            attempt_dict[attempt]["a.archive_path"]='None'
        if (attempt_dict[attempt]["t.status"] is None):
            if (attempt_dict[attempt]["t.end_time"] is None):
                attempt_dict[attempt]["end_time"]="Processing"
                attempt_dict[attempt]["proctime"]=-1
                attempt_dict[attempt]["totalwall"]=-1
                attempt_dict[attempt]["tdays"]=-1
                attempt_dict[attempt]["status"]=-1
        else:
            attempt_dict[attempt]["status"]=attempt_dict[attempt]["t.status"]
            if (attempt_dict[attempt]["t.end_time"] is None):
                attempt_dict[attempt]["end_time"]="Failed?"
            else:            
                attempt_dict[attempt]["end_time"]=attempt_dict[attempt]["t.end_time"].strftime("%Y%m%d:%H%M%S")
            if ((attempt_dict[attempt]["t.end_time"] is None)or(attempt_dict[attempt]["t.start_time"] is None)):
                attempt_dict[attempt]["proctime"]=-1.
            else:
                attempt_dict[attempt]["proctime"]=(attempt_dict[attempt]["t.end_time"]-attempt_dict[attempt]["t.start_time"]).total_seconds()/3600.0
            if ((attempt_dict[attempt]["t.end_time"] is None)or(attempt_dict[attempt]["a.submittime"] is None)):
                attempt_dict[attempt]["totalwall"]=-1.
            else:
                attempt_dict[attempt]["totalwall"]=(attempt_dict[attempt]["t.end_time"]-attempt_dict[attempt]["a.submittime"]).total_seconds()/3600.0
            if (attempt_dict[attempt]["t.end_time"] is None):
                attempt_dict[attempt]["tdays"]=-1.
            else:
                attempt_dict[attempt]["tdays"]=(attempt_dict[attempt]["t.end_time"]-tfirst_submit).total_seconds()/(3600.*24.)

#        if (attempt_dict[attempt]['r.pipeline']=='sne'):
#            field=attempt_dict[attempt]['a.unitname'].split("_")[1]
#            band=attempt_dict[attempt]['a.unitname'].split("_")[2]
#            if (field in SN_Shallow):
#                if (band in ['g','r','i']):
#                    attempt_dict[attempt]['pvar']="shallow-gri"
#                elif (band in ['z']):
#                    attempt_dict[attempt]['pvar']="shallow-z"
#                else:
#                    attempt_dict[attempt]['pvar']="shallow-other"
#            elif (field in SN_Deep):
#                attempt_dict[attempt]['pvar']="deep-"+band
#            else:
#                attempt_dict[attempt]['pvar']="unknown"
#        else:
#            attempt_dict[attempt]['pvar']="normal"
        if (attempt_dict[attempt]["status"] == 0):
            attempt_dict[attempt]['pvar']="nominal"
        else:
            if (attempt_dict[attempt]["status"] > 0):
                attempt_dict[attempt]['pvar']="failed"
            else:
                if (attempt_dict[attempt]["t.end_time"] is None):
                    attempt_dict[attempt]['pvar']="nominal"
                else:
                    attempt_dict[attempt]['pvar']="failed"
            
#
#   Print full summary of all runs.
#

    print("{:12s} {:10s} {:15s} {:6s} {:2s} {:8s} {:12s} {:15s} {:15s} {:7s} {:7s} {:8s} {:6s} {:s} ".format("#"," "," "," "," "," "," "," "," ","  Proc","Elapsed"," "," "," "))
    print("{:12s} {:10s} {:15s} {:6s} {:2s} {:8s} {:12s} {:15s} {:15s} {:7s} {:7s} {:8s} {:6s} {:s} ".format("# attempt_id","exec_host","fld-band-seq","reqnum","a#","Campaign","Pipeline","  Submit Time","    End Time","  [s]","  [d]","  State","Status","  Filepath"))
    for attempt in attempt_list:
#        print attempt_dict[attempt]
#        print attempt_dict[attempt]['exec_host']
        print("{att_id:12d} {e_host:10s} {a_unit:15s} {a_reqn:6d} {a_attn:2d} {campgn:8s} {pipeln:12s} {s_time:15s} {e_time:15s} {p_time:7.2f} {t_days:7.3f} {dstate:8s} {status:6d} {a_path:s} ".format(
            att_id=attempt,
            e_host=attempt_dict[attempt]["exec_host"][:10],
            a_unit=attempt_dict[attempt]["a.unitname"],
            a_reqn=attempt_dict[attempt]["a.reqnum"],
            a_attn=attempt_dict[attempt]["a.attnum"],
            campgn=attempt_dict[attempt]["r.campaign"][:8],
            pipeln=attempt_dict[attempt]["r.pipeline"][:12],
            s_time=attempt_dict[attempt]["start_time"],
            e_time=attempt_dict[attempt]["end_time"],
            p_time=attempt_dict[attempt]["proctime"],
            t_days=attempt_dict[attempt]["tdays"],
            dstate=attempt_dict[attempt]["a.data_state"][:7],
            status=attempt_dict[attempt]["status"],
            a_path=attempt_dict[attempt]["a.archive_path"]))
#        print attempt_dict[attempt]["a.subpipeprod"],attempt_dict[attempt]["a.subpipever"]
    if (len(attempt_list)<1):
        print("# No results")
    print("# End summary ")

#
#   Throughput QA plots
#
#    if (QAplotFlag):
#        count=0
#        cstart=[]
#        ctime=[]
#        clist=[]
#        color=[]
#        cval=[]
#        for attempt in attempt_list:
#            if (attempt_dict[attempt]['status']>=0):
#                count=count+1
#                if ((attempt_dict[attempt]['tdays']>0)and(attempt_dict[attempt]['proctime']>0)):
#                    if (attempt_dict[attempt]['t.start_time'] is not None):
#                        cstart.append(attempt_dict[attempt]['t.start_time'])
##                        cstart.append(attempt_dict[attempt]['tdays'])
#                        ctime.append(attempt_dict[attempt]['proctime'])
##                        cval.append(attempt_dict[attempt]['r.campaign']+"_"+attempt_dict[attempt]['r.pipeline']+"_"+attempt_dict[attempt]['pvar'])
##                        cval.append(attempt_dict[attempt]['r.pipeline'])
#                        cval.append(attempt_dict[attempt]['r.pipeline']+"_"+attempt_dict[attempt]['pvar'])
#                        clist.append(count)
#        pdata={'x1':cstart,'y1':clist,'y2':ctime,'color':cval}
#        pfname='%s_job_throughput.png' % ( args.QAplot )
#        yeahIworked=qaplot_job_throughput(pdata,pfname)
#        print yeahIworked

#
#   Binned/histogram summarys
#

#
#   Find min/max for data (for setting limits)
#


    if (QAplotFlag):
#
#       Throughput plot preliminary munging.
#
        cstart=[]
        ctime=[]
        color=[]
        cval=[]
        for attempt in attempt_list:
            if (attempt_dict[attempt]['t.start_time'] is not None):
                cstart.append(attempt_dict[attempt]['t.start_time'])
                ctime.append(attempt_dict[attempt]['proctime'])
#                cval.append(attempt_dict[attempt]['r.campaign']+"_"+attempt_dict[attempt]['r.pipeline']+"_"+attempt_dict[attempt]['pvar'])
                cval.append(attempt_dict[attempt]['r.pipeline']+"_"+attempt_dict[attempt]['pvar'])

#
#       Obtain time ranges then setup binning.
#
        TNcstart=matplotlib.dates.date2num(cstart)
        TNcstart_min=min(TNcstart)
        TNcstart_max=max(TNcstart)
        TNcstart_range=TNcstart_max-TNcstart_min

        TimeBins=numpy.arange(TNcstart_min,TNcstart_max+TimeBin,TimeBin)
#        print TNcstart_min,TNcstart_max
#        print TimeBins
        TimeBinCent=numpy.zeros(numpy.shape(TimeBins),dtype=float)
        for ibin, t1 in enumerate(TimeBins):
            if (ibin < len(TimeBins)-1):
                TimeBinCent[ibin]=0.5*(TimeBins[ibin]+TimeBins[ibin+1])
            else:
                # For the last bin extrapolate forward (just to make sure there is a "sensible" value rather than 0 or some other nasty bin)
                TimeBinCent[ibin]=TimeBins[ibin]+0.5*(TimeBins[ibin]-TimeBins[ibin-1])
#        ZeroInit=numpy.zeros(numpy.shape(TimeBins),dtype=int)
        RunTime_Stats={}
#
#       
#
#        TimeStart=[]
#        TimeSuccess=[]
#        TimeFailure=[]
        PipeSummary={}
        for attempt in attempt_list:
            if (attempt_dict[attempt]['t.start_time'] is not None):
                CurPipe=attempt_dict[attempt]['r.pipeline']+"_"+attempt_dict[attempt]['pvar']
                if (CurPipe not in PipeSummary):
                    PipeSummary[CurPipe]={}
                    PipeSummary[CurPipe]['start_list']=[]
                    PipeSummary[CurPipe]['success_list']=[]
                    PipeSummary[CurPipe]['failure_list']=[]
                    PipeSummary[CurPipe]['running_list']=[]
                    PipeSummary[CurPipe]['runtime_stats']={}
    
                TN_att_start=matplotlib.dates.date2num(attempt_dict[attempt]['t.start_time'])
                PipeSummary[CurPipe]['start_list'].append(TN_att_start)
                if (attempt_dict[attempt]['t.end_time'] is not None):
                    TN_att_stop=matplotlib.dates.date2num(attempt_dict[attempt]['t.end_time'])
                    if (attempt_dict[attempt]['status'] == 0):
                        PipeSummary[CurPipe]['success_list'].append(TN_att_stop)
#                       Place processing time into time-bin for statistical analysis of runtime
                        start_bin=numpy.digitize([TN_att_start],TimeBins)
                        if (start_bin[0] not in PipeSummary[CurPipe]['runtime_stats']):
                            PipeSummary[CurPipe]['runtime_stats'][start_bin[0]]=[]
                        PipeSummary[CurPipe]['runtime_stats'][start_bin[0]].append(attempt_dict[attempt]["proctime"])
                    else:
                        PipeSummary[CurPipe]['failure_list'].append(TN_att_stop)
                else:
#                    TN_att_stop=TNcstart_max
                    TN_att_stop=TN_att_start+2.0
#                    if (TN_att_stop > TNcstart_max):
#                        TN_att_stop=TNcstart_max

#
#               Work out the range over which an attempt was running and insert into the running structure
#
                run_range=numpy.where(numpy.logical_and((TimeBins < TN_att_stop+TimeBin),(TimeBins > TN_att_start-TimeBin)))
#                print run_range
                for CentTime in TimeBinCent[run_range]:
                    if (CurPipe == "sne"):
                        PipeSummary[CurPipe]['running_list'].append(CentTime)
#                        for icnt in range(0,60):
#                            PipeSummary[CurPipe]['running_list'].append(CentTime)
                    else:                            
                        PipeSummary[CurPipe]['running_list'].append(CentTime)
#
#       Analyze statistics in each time bins (for processing time)
#
        Ptime_tval=[]
        Ptime_mval=[]
        Ptime_cval=[]
        for CurPipe in PipeSummary:
            for tbin in PipeSummary[CurPipe]['runtime_stats']:
                if (len(PipeSummary[CurPipe]['runtime_stats'][tbin]) > 0):
                    Ptime_tval.append(TimeBinCent[tbin])
                    Ptime_mval.append(numpy.median(numpy.array(PipeSummary[CurPipe]['runtime_stats'][tbin])))
                    Ptime_cval.append(CurPipe)

#
        pdata={'x1':TimeBins,'y1':PipeSummary,'x2':TNcstart,'y2':ctime,'color':cval,'x3':Ptime_tval,'y3':Ptime_mval,'c3':Ptime_cval}
        pfname='%s_job_throughput2.png' % ( args.QAplot )
        yeahIworked=qaplot_job_throughput2(pdata,pfname)
#        print yeahIworked



    exit(0)
#
#   Transfer
#     by prepared query.
#
    t0=time.time()
    use_Prepared=True
#    use_Prepared=False
    if (use_Prepared):
        queryitems = ["s.task_id","s.name","s.slot","s.request_time","s.grant_time","s.release_time"]

        querylist = ",".join(queryitems)
        coldict={}
        for index, item in enumerate(queryitems):
            coldict[item]=index
#        query = """select %s from %sseminfo s, %stask t, %spfw_attempt a where a.reqnum=%s and a.unitname=%s and a.attnum=%s and a.task_id=t.root_task_id and t.id=s.task_id order by s.request_time""" % ( querylist, db_Schema, db_Schema, db_Schema, dbh.get_named_bind_string('reqnum'), dbh.get_named_bind_string('unitname'), dbh.get_named_bind_string('attnum')) 
        query = """select %s from %sseminfo s, %stask t, %spfw_attempt a where a.id=%s and a.task_id=t.root_task_id and t.id=s.task_id order by s.request_time""" % ( querylist, db_Schema, db_Schema, db_Schema, dbh.get_named_bind_string('pfw_attempt_id')) 
        cur.prepare(query)
        if (args.verbose):
            print query

    NumAttemptProcessed=0
    for attempt in attempt_list:
        NumAttemptProcessed=NumAttemptProcessed+1
        if (not(args.verbose)):
            if ( ( NumAttemptProcessed % 100 )==0 ):
                print "Working on Attempt %d of %d " % (NumAttemptProcessed,len(attempt_list))

        if (use_Prepared):
#            query_parms={'reqnum':attempt_dict[attempt]['a.reqnum'],'unitname':attempt_dict[attempt]['a.unitname'],'attnum':attempt_dict[attempt]['a.attnum']}
            query_parms={'pfw_attempt_id':attempt}
            cur.arraysize = 1000 # get 1000 at a time when fetching
            cur.execute(None, query_parms)
        else:
            queryitems = ["s.task_id","s.name","s.slot","s.request_time","s.grant_time","s.release_time"]

            querylist = ",".join(queryitems)
            coldict={}
            for index, item in enumerate(queryitems):
                coldict[item]=index

            query = """select %s from %sseminfo s, %stask t, %spfw_attempt a where a.reqnum=%d and a.unitname='%s' and a.attnum=%d and a.task_id=t.root_task_id and t.id=s.task_id order by s.request_time""" % ( querylist, db_Schema, db_Schema, db_Schema, attempt_dict[attempt]['a.reqnum'], attempt_dict[attempt]['a.unitname'], attempt_dict[attempt]['a.attnum'] )
            
            if args.verbose:
                if (attempt == attempt_list[0]):
                    print query
            cur.execute(query)
        
#        query = """select %s from %sseminfo s, %stask t, %spfw_attempt a where a.reqnum=%s and a.unitname=%s and a.attnum=%s and a.task_id=t.root_task_id and t.id=s.task_id order by s.request_time""" % ( querylist, db_Schema, db_Schema, db_Schema, dbh.get_named_bind_string('reqnum'), dbh.get_named_bind_string('unitname'), dbh.get_named_bind_string('attnum')) 

        trans_list=[]
        for item in cur:
            d = dict(zip(queryitems, item))
            trans_list.append(d)
        if (args.verbose):
            if (attempt_dict[attempt]['a.id'] is None):
                print(" Attempt {:d}:{:s}:{:d} found # seminfo {:d}".format(attempt_dict[attempt]['a.reqnum'], attempt_dict[attempt]['a.unitname'], attempt_dict[attempt]['a.attnum'],len(trans_list)))
            else:
                print(" Attempt {:d}({:d}:{:s}:{:d}) found # seminfo {:d}".format(attempt,attempt_dict[attempt]['a.reqnum'], attempt_dict[attempt]['a.unitname'], attempt_dict[attempt]['a.attnum'],len(trans_list)))
        if args.verbose:
            if (attempt == attempt_list[0]):
                for trans in trans_list:
                    print trans

        trans_info={'in':{'wait':[],'xfer':[],'pend':[]},'out':{'wait':[],'xfer':[],'pend':[]},'other':{'wait':[],'xfer':[],'pend':[]}}
        for trans in trans_list:
            if (re.search("input",trans['s.name']) is not None):
                trans_queue='in'
            elif (re.search("output",trans['s.name']) is not None):
                trans_queue='out'
            elif (re.search("alphaftp5",trans['s.name']) is not None):
                trans_queue='in'
            elif (re.search("alphaftp10",trans['s.name']) is not None):
                trans_queue='out'
            else:
                trans_queue='other'
            if (trans_queue=='other'): print trans['s.name']
            if (trans['s.release_time'] is None):
                trans_info[trans_queue]['pend'].append(1)
            else:
                try:
                    wait_time=(trans["s.grant_time"]-trans["s.request_time"]).total_seconds()
                    xfer_time=(trans["s.release_time"]-trans["s.grant_time"]).total_seconds()
                except:
                    print("Attempt to get timing info for a transfer failed")
                    wait_time=-1.
                    xter_time=-1.
                if ((wait_time>=0.)and(xfer_time>=0.)):
                    trans_info[trans_queue]['wait'].append(wait_time)
                    trans_info[trans_queue]['xfer'].append(xfer_time)
                else:
                    trans_info[trans_queue]['pend'].append(1)

        attempt_dict[attempt]['transfer']={'in':{},'out':{},'other':{}}
        for queue in ['in','out','other']:
            num_val=[len(trans_info[queue]['wait']),len(trans_info[queue]['xfer']),len(trans_info[queue]['pend'])]
            if (args.verbose):
                print queue,num_val
            if ((num_val[0]>0)and(num_val[1]>0)):
                a_wait=numpy.array(trans_info[queue]['wait'])
                a_xfer=numpy.array(trans_info[queue]['xfer'])
                if (num_val[2]>0):
                    attempt_dict[attempt]['transfer'][queue]['state']='Pending'
                else:
                    attempt_dict[attempt]['transfer'][queue]['state']='Finished'
                attempt_dict[attempt]['transfer'][queue]['num']=len(trans_info[queue]['wait'])
                attempt_dict[attempt]['transfer'][queue]['total_wait']=numpy.sum(a_wait)/3600.0
                attempt_dict[attempt]['transfer'][queue]['med_wait']=numpy.median(a_wait)/3600.0
                attempt_dict[attempt]['transfer'][queue]['total_xfer']=numpy.sum(a_xfer)/3600.0
                attempt_dict[attempt]['transfer'][queue]['med_xfer']=numpy.median(a_xfer)/3600.0

#                if (len(a_wait)>0):
#                    attempt_dict[attempt]['transfer'][queue]['total_wait']=numpy.sum(a_wait)/3600.0
#                    attempt_dict[attempt]['transfer'][queue]['med_wait']=numpy.median(a_wait)/3600.0
#                else:
#                    attempt_dict[attempt]['transfer'][queue]['total_wait']=-1.
#                    attempt_dict[attempt]['transfer'][queue]['med_wait']=-1.
#                if (len(a_xfer)>0):
#                    attempt_dict[attempt]['transfer'][queue]['total_xfer']=numpy.sum(a_xfer)/3600.0
#                    attempt_dict[attempt]['transfer'][queue]['med_xfer']=numpy.median(a_xfer)/3600.0
#                else:
#                    attempt_dict[attempt]['transfer'][queue]['total_xfer']=-1.
#                    attempt_dict[attempt]['transfer'][queue]['med_xfer']=-1.

#                med_val=[numpy.median(a_wait),numpy.median(a_xfer)]
#                med_val=[numpy.median(a_wait),numpy.median(a_xfer)]
#                avg_val=[numpy.average(a_wait),numpy.average(a_xfer)]

#                print(" {:5s} {:8s} {:6d} {:6.2f} {:6.2f} {:6.2f} {:6.2f} ".format(queue,trans_state,num_val[0],sum_val[0],sum_val[1],med_val[0],med_val[1]))

    t1=time.time()
    print("Query Time: {:.1f} ".format(t1-t0))
#
#   Transfer QA plots
#

    if (QAplotFlag):
        cstart=[]
        twait_in=[]
        twait_out=[]
        txfer_in=[]
        txfer_out=[]
        cval=[]
#        print len(attempt_list)
        for attempt in attempt_list:
            if (attempt_dict[attempt]['status']>=0):
#                print "1  ",attempt,attempt_dict[attempt]['tdays'],attempt_dict[attempt]['proctime']
                if ((attempt_dict[attempt]['tdays']>0)and(attempt_dict[attempt]['proctime']>0)):
#                    print "2    ",attempt,attempt_dict[attempt]['transfer']['in'],attempt_dict[attempt]['transfer']['out']
                    if (('num' in attempt_dict[attempt]['transfer']['in'])and('num' in attempt_dict[attempt]['transfer']['out'])):
                        cstart.append(attempt_dict[attempt]['tdays'])
                        twait_in.append(attempt_dict[attempt]['transfer']['in']['total_wait'])
                        twait_out.append(attempt_dict[attempt]['transfer']['out']['total_wait'])
                        txfer_in.append(attempt_dict[attempt]['transfer']['in']['total_xfer'])
                        txfer_out.append(attempt_dict[attempt]['transfer']['out']['total_xfer'])
#                        cval.append(attempt_dict[attempt]['r.campaign']+"_"+attempt_dict[attempt]['r.pipeline'])
                        cval.append(attempt_dict[attempt]['r.campaign']+"_"+attempt_dict[attempt]['r.pipeline']+"_"+attempt_dict[attempt]['pvar'])
        pdata={'x1':cstart,'y1':twait_in,'y2':twait_out,'y3':txfer_in,'y4':txfer_out,'color':cval}
        pfname='%s_transfer.png' % ( args.QAplot )
        yeahIworked=qaplot_transfer1(pdata,pfname)
        print yeahIworked

#
#   Transfer

    exit(0)    
#
#   Done at this point if "--terse" was chosen.  Otherwise continue and find out specific info about each run.
#
    if (not(args.terse)):
        print("#")
        print("#")
#
#       Form a dictionary to hold the details of the processing
##
        proc_dict={}
        for attempt in attempt_list:
            proc_attempt="%d:%s:%d" % (attempt['reqnum'],attempt['unitname'],attempt['attnum'])
            print("#############################################################")
            print("# {:s}".format(proc_attempt))
            proc_dict[proc_attempt]={}
#
#           Get list of modules that were/are to be executed.
#
            queryitems = ["b.blknum", "lower(b.modulelist)"]
            querylist = ",".join(queryitems)
            coldict={}
            for index, item in enumerate(queryitems):
                coldict[item]=index
            query = """select %s from %spfw_block b where b.reqnum=%d and b.unitname='%s' and b.attnum=%d order by b.blknum """ % ( querylist, db_Schema, attempt['reqnum'],attempt['unitname'],attempt['attnum'])

            if args.verbose:
                print query
            cur.arraysize = 1000 # get 1000 at a time when fetching
            cur.execute(query)

            mod_list=[]
            for item in cur:
                for mod in item[coldict["lower(b.modulelist)"]].split(','):
                    proc_dict[proc_attempt][mod]={}
                    proc_dict[proc_attempt][mod]['fail']=0
                    proc_dict[proc_attempt][mod]['pass']=0
                    proc_dict[proc_attempt][mod]['proc']=0
                    proc_dict[proc_attempt][mod]['fail_jk']=[]
                    proc_dict[proc_attempt][mod]['proc_jk']=[]
                    mod_list.append(mod)
            proc_dict[proc_attempt]['modlist']=mod_list

#
#           Now look at specific of what has been run (and check for failures)
#
            queryitems = ["w.modname","e.task_id","e.name","e.status","j.jobkeys"]
            querylist = ",".join(queryitems)
            coldict={}
            for index, item in enumerate(queryitems):
                coldict[item]=index
            query = """select %s from %spfw_wrapper w, %spfw_exec e, %spfw_job j where w.reqnum=%d and w.unitname='%s' and w.attnum=%d and w.reqnum=e.reqnum and w.unitname=e.unitname and w.attnum=e.attnum and e.wrapnum=w.wrapnum and w.reqnum=j.reqnum and w.unitname=j.unitname and w.attnum=j.attnum and w.jobnum=j.jobnum """ % ( querylist, db_Schema, db_Schema, db_Schema, attempt['reqnum'],attempt['unitname'],attempt['attnum'])

            if args.verbose:
                print query
            cur.arraysize = 1000 # get 1000 at a time when fetching
            cur.execute(query)

            for item in cur:
                mname=item[coldict["w.modname"]].lower()
                if (mname not in proc_dict[proc_attempt]):
                    print "# Warning: Missing module ",mname," (added)"
                    proc_dict[proc_attempt][mname]={}
                    proc_dict[proc_attempt][mname]['fail']=0
                    proc_dict[proc_attempt][mname]['pass']=0
                    proc_dict[proc_attempt][mname]['proc']=0
                    proc_dict[proc_attempt][mname]['fail_jk']=[]
                    proc_dict[proc_attempt][mname]['proc_jk']=[]
                    proc_dict[proc_attempt]['modlist'].append(mname)
                if (item[coldict["e.status"]] is not None):
                    if (int(item[coldict["e.status"]])!=0):
                        proc_dict[proc_attempt][mname]['fail']=proc_dict[proc_attempt][mname]['fail']+1
                        proc_dict[proc_attempt][mname]['fail_jk'].append(item[coldict["j.jobkeys"]])
                    else:
                        proc_dict[proc_attempt][mname]['pass']=proc_dict[proc_attempt][mname]['pass']+1
                else:
                    proc_dict[proc_attempt][mname]['proc']=proc_dict[proc_attempt][mname]['proc']+1
                    proc_dict[proc_attempt][mname]['proc_jk'].append(item[coldict["j.jobkeys"]])
#                    print item[coldict["j.jobkeys"]]
            mod_list=[]
            mod_unfinished=0
            for mod in proc_dict[proc_attempt]['modlist']:
                if ((proc_dict[proc_attempt][mod]['fail']>0)or(proc_dict[proc_attempt][mod]['proc']>0)or(args.full)):
                    mod_list.append(mod)
                if ((proc_dict[proc_attempt][mod]['fail']>0)or(proc_dict[proc_attempt][mod]['proc']>0)):
                    mod_unfinished=mod_unfinished+1
            if (mod_unfinished==0):
                print("#  No failures or running jobs detected")
            for mod in mod_list:
                fail_sum_jk=""
                proc_sum_jk=""
                if (proc_dict[proc_attempt][mod]['fail']>0):
                    fail_sum_jk="Failed: " + ",".join(proc_dict[proc_attempt][mod]['fail_jk'])
                if (proc_dict[proc_attempt][mod]['proc']>0):
                    proc_sum_jk="Running: " + ",".join(proc_dict[proc_attempt][mod]['proc_jk'])
                print("   {:5d} {:5d} {:5d} {:30s} {:s} {:s} ".format(proc_dict[proc_attempt][mod]['pass'],proc_dict[proc_attempt][mod]['fail'],proc_dict[proc_attempt][mod]['proc'],mod,fail_sum_jk,proc_sum_jk))

    exit(0)
