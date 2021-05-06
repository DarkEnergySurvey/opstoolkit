#! /usr/bin/env python3

"""
Simple application to get exposure-based astrometry QA from a COADD attempt.

Imported from SVN (last change was rev 44367, October 11, 2016 by rgruendl)
Python3 update: RAG, Sept, 24th 2020
"""

########################
def QAplt_astrom_group(PlotFile, PlotTitle, Data, Clusters, verbose=0):
    """
    Function to produce a QA plot showing exposure-by-exposure delta offsets
    from astrometric solution.

    inputs:
        PlotFile:   filename where the output plot will be written (can include 
                        relative path)
        Data:       Custom list of dictionaries that holds data for the plots.  
                        x: list of dx [arcseconds]
                        y: list of dy [arcseconds]
                        band: band for each exposure
        verbose:    Provide verbose output (curently there is none).

    OUTPUT: 
        Nothing is returned to the calling program (a PNG file is written where directed)
    """

    bandlist=['u','g','r','i','z','Y']
    bcolor={'u':'cyan','g':'blue','r':'green','i':'orange','z':'red','Y':'maroon'}

    bax=0.125
    lax=0.125
    bdax=0.8
    ldax=0.8
    fig=plt.figure()
    ax=fig.add_axes([bax,lax,bdax,ldax])

    xmin=Data[0]['x']
    xmax=Data[0]['x']
    ymin=Data[0]['y']
    ymax=Data[0]['y']
#    print("{:7.2f} {:7.2f} {:7.2f} {:7.2f} ".format(xmin,xmax,ymin,ymax))
    for obj in Data:
        if (obj['x']<xmin):
            xmin=obj['x']
        if (obj['x']>xmax):
            xmax=obj['x']
        if (obj['y']<ymin):
            ymin=obj['y']
        if (obj['y']>ymax):
            ymax=obj['y']

#    print("{:7.2f} {:7.2f} {:7.2f} {:7.2f} ".format(xmin,xmax,ymin,ymax))
    xrange=1.1*(xmax-xmin)
    yrange=1.1*(ymax-ymin)
    if (xrange < 50.):
        xrange=25.0
        xcen=0.5*(xmin+xmax)
        xmin=xcen-0.5*xrange
        xmax=xcen+0.5*xrange
    if (yrange < 50.):
        yrange=25.0
        ycen=0.5*(ymin+ymax)
        ymin=ycen-0.5*yrange
        ymax=ycen+0.5*yrange
    if (xrange > yrange):
        xcen=0.5*(xmin+xmax)
        xmin=xcen-0.5*xrange
        xmax=xcen+0.5*xrange
        ycen=0.5*(ymin+ymax)
        ymin=ycen-0.5*xrange
        ymax=ycen+0.5*xrange
        inset_use='y'
    else:
        xcen=0.5*(xmin+xmax)
        xmin=xcen-0.5*yrange
        xmax=xcen+0.5*yrange
        ycen=0.5*(ymin+ymax)
        ymin=ycen-0.5*yrange
        ymax=ycen+0.5*yrange
        inset_use='x'
#    print("{:7.2f} {:7.2f} {:7.2f} {:7.2f} ".format(xmin,xmax,ymin,ymax))

    ax.set_xlim([xmin,xmax])
    ax.set_ylim([ymin,ymax])
    ax.set_xlabel('dx [arcsec]')
    ax.set_ylabel('dy [arcsec]')
    ax.set_title(PlotTitle)

    for band in bandlist:
        xdata=[obj['x'] for obj in Data if obj['band'] == band]
        ydata=[obj['y'] for obj in Data if obj['band'] == band]
        if (len(xdata) > 0):
            ax.scatter(xdata,ydata,marker='.',color=bcolor[band])

#
#   Overplot ellipses (assuming 10 sigma membership among pointing)
#
    a=numpy.arange(0.,360.,0.25)
    for G in Clusters:
        xell=G['x0']+10.*G['dx0']*numpy.cos(a*numpy.pi/180.0)
        yell=G['y0']+10.*G['dy0']*numpy.sin(a*numpy.pi/180.0)
        ax.plot(xell,yell,color='black',lw=1,ls='dashed')

#    ells = [Ellipse(xy=(G['x0'],G['y0']), width=10.0*G['dx0'], height=10.0*G['dy0'], angle=0.0, edgecolor='black', fill=False, ls='dotted', fc='None', lw=2) for G in Clusters]
#    ells = [Ellipse(xy=(bax+(G['x0']-xmin)*bdax/(xmax-xmin),lax+(G['y0']-xmin)*ldax/(ymax-ymin)),
#                    width=10.0*G['dx0']*bdax/(xmax-xmin),
#                    height=10.0*G['dy0']*ldax/(ymax-ymin),
#                    angle=0.0, edgecolor='black', fill=False, ls='dotted', fc=None, lw=2) for G in Clusters]
#    ells.append(Ellipse(xy=(0.0,-30.0), width=5.0, height=2.0, edgecolor='black', fill=False, ls='dotted',fc='None', lw=2))
#    for e in ells:
#        ax.add_patch(e)


#
#   Form insets
#
    ninset=len(Clusters)
    if (inset_use == 'x'):
        if (ninset < 5):
            dv=0.25
            inpos={ 1:{'x0':0.20,'y0':0.20,'dx':dv,'dy':dv},
                    2:{'x0':0.65,'y0':0.20,'dx':dv,'dy':dv},
                    3:{'x0':0.20,'y0':0.65,'dx':dv,'dy':dv},
                    4:{'x0':0.65,'y0':0.65,'dx':dv,'dy':dv}}
        if ((ninset > 4)and(ninset < 7)):
            dv=0.25
            inpos={ 1:{'x0':0.10,'y0':0.10,'dx':dv,'dy':dv},
                    2:{'x0':0.40,'y0':0.10,'dx':dv,'dy':dv},
                    3:{'x0':0.70,'y0':0.10,'dx':dv,'dy':dv},
                    4:{'x0':0.10,'y0':0.65,'dx':dv,'dy':dv},
                    5:{'x0':0.40,'y0':0.65,'dx':dv,'dy':dv},
                    6:{'x0':0.70,'y0':0.65,'dx':dv,'dy':dv}}
    else:
        if (ninset < 5):
            dv=0.25
            inpos={ 1:{'x0':0.20,'y0':0.20,'dx':dv,'dy':dv},
                    2:{'x0':0.65,'y0':0.20,'dx':dv,'dy':dv},
                    3:{'x0':0.20,'y0':0.65,'dx':dv,'dy':dv},
                    4:{'x0':0.65,'y0':0.65,'dx':dv,'dy':dv}}
        if ((ninset > 4)and(ninset < 7)):
            dv=0.25
            inpos={ 1:{'x0':0.10,'y0':0.10,'dx':dv,'dy':dv},
                    2:{'x0':0.40,'y0':0.10,'dx':dv,'dy':dv},
                    3:{'x0':0.70,'y0':0.10,'dx':dv,'dy':dv},
                    4:{'x0':0.10,'y0':0.65,'dx':dv,'dy':dv},
                    5:{'x0':0.40,'y0':0.65,'dx':dv,'dy':dv},
                    6:{'x0':0.70,'y0':0.65,'dx':dv,'dy':dv}}
    bax=0.125
    lax=0.125
    bdax=0.8
    ldax=0.8
    in_cnt=0
    for Group in sorted(Clusters, key=lambda k: (k['y0'])):
        in_cnt=in_cnt+1
        ax_inset=fig.add_axes([inpos[in_cnt]['x0'],inpos[in_cnt]['y0'],inpos[in_cnt]['dx'],inpos[in_cnt]['dy']])
        gdx=Group['dx0']
        if (Group['dy0']>Group['dx0']):
            gdx=Group['dy0']
        gxmin=Group['x0']-20.0*gdx
        gxmax=Group['x0']+20.0*gdx
        gymin=Group['y0']-20.0*gdx
        gymax=Group['y0']+20.0*gdx
        for band in bandlist:
            xdata=[obj['x'] for obj in Data if ((obj['band'] == band)and(obj['x']>gxmin)and(obj['x']<gxmax)and(obj['y']>gymin)and(obj['y']<gymax))]
            ydata=[obj['y'] for obj in Data if ((obj['band'] == band)and(obj['x']>gxmin)and(obj['x']<gxmax)and(obj['y']>gymin)and(obj['y']<gymax))]
            if (len(xdata) > 0):
                ax_inset.scatter(xdata,ydata,marker='.',color=bcolor[band])
                ax_inset.set_xlim([gxmin,gxmax])
                ax_inset.set_ylim([gymin,gymax])
                for tick in ax_inset.xaxis.get_ticklabels():
                    tick.set_fontsize('x-small')
                for tick in ax_inset.yaxis.get_ticklabels():
                    tick.set_fontsize('x-small')
        a=numpy.arange(0.,360.,0.25)
        for G in Clusters:
            xell=G['x0']+10.*G['dx0']*numpy.cos(a*numpy.pi/180.0)
            yell=G['y0']+10.*G['dy0']*numpy.sin(a*numpy.pi/180.0)
            ax_inset.plot(xell,yell,color='black',lw=1,ls='dashed')
#        for e in ells:
#            ax_inset.add_artist(e)
#            e.set_clip_box(ax_inset.bbox)

    plt.savefig(PlotFile,dpi=90)

    return 0


########################
def find_clusters(Data,dfloor=3.,ncluster=2,verbose=0):
    """
    Analyze set to look for clustered points from astrometric solution.

    inputs:
        PlotFile:   filename where the output plot will be written (can include 
                        relative path)
        Data:       Custom list of dictionaries that holds data for the plots.  
                        x: list of dx [arcseconds]
                        y: list of dy [arcseconds]
                        band: band for each exposure
        verbose:    Provide verbose output (curently there is none).

    OUTPUT: 
        List of Dictionaries for Clusters Found
    """

    nbins=int(5*ncluster)
#
#   Beginnigs of old attempt working from radial/polar coordinates
#
#    dist=numpy.array([math.sqrt(obj['x']*obj['x']+obj['y']*obj['y']) for obj in Data])
#    ang=numpy.array([180./3.1415927*math.atan2(obj['y'],obj['x']) for obj in Data])
#    dhist,dbins=numpy.histogram(dist,nbins)
#    indhist=numpy.argsort(dhist)
#    print dhist
#    print dbins
#    print indhist

    xdata=numpy.array([obj['x'] for obj in Data])
    ydata=numpy.array([obj['y'] for obj in Data])
    d2hist,d2xbins,d2ybins=numpy.histogram2d(xdata,ydata,nbins)

    if (verbose>2):
        print(d2hist)

    cluster=[]
    FindMoreClusters=True
    icluster=0
    while (FindMoreClusters):
        icluster=icluster+1
        indhist=numpy.argsort(numpy.reshape(d2hist,d2hist.size,))
#        if ((icluster == 1)and(verbose>2)):
#            print indhist
#        print d2hist
#        print d2xbins
#        print d2ybins
#        print indhist
        peak=indhist[-1]
        ixp=int(peak/nbins)
        iyp=peak%nbins
#
#       Remove points from furhter consideration near the peak that was identified
#
        nmember=0
        for ix in [-2,-1,0,1,2]:
            xc=ixp+ix
            if ((xc >= 0)and(xc < nbins)):
                for iy in [-2,-1,0,1,2]:
                    yc=iyp+iy
                    if ((yc >= 0)and(yc < nbins)):
                        nmember=nmember+d2hist[xc,yc]
#                        print xc,yc,d2hist[xc,yc]
                        d2hist[xc,yc]=0
#
        if (verbose>2):
            print("# Peak {:d}: init(x,y)=({:.3f},{:.3f}), init_member={:d} ".format(icluster,d2xbins[ixp],d2ybins[iyp],int(nmember)))
        cluster.append({'cnt':icluster,'x0':d2xbins[ixp],'y0':d2ybins[iyp],'n0':nmember,'xd':[],'yd':[],'rd':[]})
#
#       Check whether or not to continue:
#
        if (icluster+1>ncluster): 
            FindMoreClusters=False 
        wsm=numpy.where(d2hist>dfloor)
        if (len(wsm) < 1):
            FindMoreClusters=False 

#
#   Refine peaks
#
    if (verbose > 2):
        print("# Peak Refinement")

    for iter in [1,2,3]:
        if (verbose > 2):
            print("# Iteration {:d}:".format(iter))
            print("# ---------------------------------------")
        for obj in Data:
            ox=obj['x']
            oy=obj['y']
            dist=numpy.array([math.sqrt((ox-peak['x0'])*(ox-peak['x0'])+(oy-peak['y0'])*(oy-peak['y0'])) for peak in cluster])
            indhist=numpy.argsort(dist)
            if ('dx0' in cluster[indhist[0]]):
                cx=cluster[indhist[0]]['x0']
                cy=cluster[indhist[0]]['y0']
                cdx=cluster[indhist[0]]['dx0']
                cdy=cluster[indhist[0]]['dy0']
                cx1=cx-cdx*3.0
                cx2=cx+cdx*3.0
                cy1=cy-cdx*3.0
                cy2=cy+cdx*3.0
                if ((ox > cx1)and(ox < cx2)and(oy > cy1)and(oy < cy2)):
                    cluster[indhist[0]]['xd'].append(ox)
                    cluster[indhist[0]]['yd'].append(oy)
                    cluster[indhist[0]]['rd'].append(dist[indhist[0]])
            else:    
                cluster[indhist[0]]['xd'].append(ox)
                cluster[indhist[0]]['yd'].append(oy)
                cluster[indhist[0]]['rd'].append(dist[indhist[0]])

        new_cluster=[]
        for group in cluster:
            xd=numpy.array(group['xd'])
            yd=numpy.array(group['yd'])
            rd=numpy.array(group['rd'])
            rd_med=numpy.median(rd)
            rd_std=numpy.std(rd)
            wsm=numpy.where(numpy.logical_and(rd>rd_med-2.0*rd_std,rd<rd_med+2.0*rd_std))
            rd_med=numpy.median(rd[wsm])
            rd_std=numpy.std(rd[wsm])
            wsm=numpy.where(numpy.logical_and(rd>rd_med-2.0*rd_std,rd<rd_med+2.0*rd_std))
            xd_med=numpy.median(xd[wsm]) 
            xd_std=numpy.std(xd[wsm]) 
            yd_med=numpy.median(yd[wsm]) 
            yd_std=numpy.std(yd[wsm]) 
            if (verbose > 2):
                print("# Peak {id:d}({cnt:4d} members): {x0:.3f}+/-{dx0:.4f}, {y0:.3f}+/-{dy0:.4f}".format(
                    id=group['cnt'],
                    cnt=rd[wsm].size,
                    x0=xd_med,dx0=xd_std,
                    y0=yd_med,dy0=yd_std))
            new_cluster.append({'cnt':group['cnt'],'x0':xd_med,'y0':yd_med,'dx0':xd_std,'dy0':yd_std,'n0':rd[wsm].size,'xd':[],'yd':[],'rd':[]})
        cluster=new_cluster

#
#   Report final parameters (and reform output so elimnate the vectors holding working data.
#
    new_cluster=[]
    if (verbose > 0):        
        print("# Final set of peaks identified")
        print("# ---------------------------------------")
    for group in cluster:
        new_cluster.append({'cnt':group['cnt'],'x0':group['x0'],'y0':group['y0'],'dx0':group['dx0'],'dy0':group['dy0'],'n0':group['n0']})
        if (verbose > 0):        
            print("# Peak {id:d}({cnt:4d} members): {x0:.3f}+/-{dx0:.4f}, {y0:.3f}+/-{dy0:.4f}".format(
                id=group['cnt'],
                cnt=group['n0'],
                x0=group['x0'],dx0=group['dx0'],
                y0=group['y0'],dy0=group['dy0']))

    return new_cluster


########################
def Form2DHist(xd,yd,bsize,range=None,nbin=None,verbose=0):
    """ Form a 2d-histogram
    """
    if (range is None):
        x1=numpy.amin(xd)
        x2=numpy.amax(xd)
        y1=numpy.amin(yd)
        y2=numpy.amax(yd)
        range=[x1,x2,y1,y2]
#    xc=(x2+x1)/2.0
#    yc=(y2+y1)/2.0
    if (nbin is None):
        nbin=[int((range[1]-range[0])/bsize)+1,int((range[3]-range[2])/bsize)+1]
    hist,xbins,ybins=numpy.histogram2d(xd,yd,nbin)
    HData={'hist':hist,'xb':xbins,'yb':ybins}
#
    return HData


########################
def FindClusters(HData,MaxFind,Floor,BlankSize=0,verbose=0):
    """
    """

    if (verbose>2):
        print(HData['hist'])
    clusters=[]
    FindMoreClusters=True
    icluster=0
    while (FindMoreClusters):
        icluster=icluster+1
        indhist=numpy.argsort(numpy.reshape(HData['hist'],HData['hist'].size,))
        nxbin,nybin=HData['hist'].shape
        peak=indhist[-1]
        ixp=int(peak/nybin)
        iyp=(peak%nybin)
#
#       Remove points from furhter consideration near the peak that was identified
#
        nmember=0
        for ix in range(-BlankSize,BlankSize+1):
            xc=ixp+ix
            if ((xc >= 0)and(xc < nxbin)):
                for iy in range(-BlankSize,BlankSize+1):
                    yc=iyp+iy
                    if ((yc >= 0)and(yc < nybin)):
                        nmember=nmember+HData['hist'][xc,yc]
                        HData['hist'][xc,yc]=0
#
        if (verbose>2):
            print("# Peak {:d}: init(x,y)=({:.3f},{:.3f}), init_member={:d} ".format(icluster,HData['xb'][ixp],HData['yb'][iyp],int(nmember)))
        clusters.append({'cnt':icluster,'x0':HData['xb'][ixp],'y0':HData['yb'][iyp],'n0':nmember,'xd':[],'yd':[],'rd':[]})
#
#       Check whether or not to continue:
#
        if (icluster+1>MaxFind): 
            FindMoreClusters=False 
        wsm=numpy.where(HData['hist']>Floor)
        if (len(HData['hist'][wsm]) < 1):
            FindMoreClusters=False 

    return clusters


########################
def assign_cluster_members(cluster,Data,smatch=3.0,verbose=0):
    """
    """
    for obj in Data:
        ox=obj['x']
        oy=obj['y']
        dist=numpy.array([math.sqrt((ox-peak['x0'])*(ox-peak['x0'])+(oy-peak['y0'])*(oy-peak['y0'])) for peak in cluster])
        indhist=numpy.argsort(dist)
        if (('dx0' in cluster[indhist[0]])and(smatch is not None)):
            cx=cluster[indhist[0]]['x0']
            cy=cluster[indhist[0]]['y0']
            cdx=cluster[indhist[0]]['dx0']*smatch
            cdy=cluster[indhist[0]]['dy0']*smatch
            cx1=cx-cdx
            cx2=cx+cdx
            cy1=cy-cdx
            cy2=cy+cdx
            if ((ox > cx1)and(ox < cx2)and(oy > cy1)and(oy < cy2)):
                cluster[indhist[0]]['xd'].append(ox)
                cluster[indhist[0]]['yd'].append(oy)
                cluster[indhist[0]]['rd'].append(dist[indhist[0]])
        else:    
            cluster[indhist[0]]['xd'].append(ox)
            cluster[indhist[0]]['yd'].append(oy)
            cluster[indhist[0]]['rd'].append(dist[indhist[0]])
    return cluster


########################
def find_clusters2(Data,dfloor=3.,ncluster=4,verbose=0):
    """
    Analyze set to look for clustered points from astrometric solution.

    inputs:
        PlotFile:   filename where the output plot will be written (can include 
                        relative path)
        Data:       Custom list of dictionaries that holds data for the plots.  
                        x: list of dx [arcseconds]
                        y: list of dy [arcseconds]
                        band: band for each exposure
        verbose:    Provide verbose output (curently there is none).

    OUTPUT: 
        List of Dictionaries for Clusters Found
    """

#
#   Beginnigs of old attempt working from radial/polar coordinates
#
#    dist=numpy.array([math.sqrt(obj['x']*obj['x']+obj['y']*obj['y']) for obj in Data])
#    ang=numpy.array([180./3.1415927*math.atan2(obj['y'],obj['x']) for obj in Data])
#    dhist,dbins=numpy.histogram(dist,nbins)
#    indhist=numpy.argsort(dhist)
#    print dhist
#    print dbins
#    print indhist

    print("##########################################")
    print("# Begin alternate algorithm ")

    prebin=5.0
    xdata=numpy.array([obj['x'] for obj in Data])
    ydata=numpy.array([obj['y'] for obj in Data])
    Hist0=Form2DHist(xdata,ydata,prebin)
    cluster0=FindClusters(Hist0,ncluster,dfloor,BlankSize=2,verbose=verbose)
    cluster0=assign_cluster_members(cluster0,Data,smatch=None,verbose=verbose)
#
    if (verbose > 2):
        print("# ")
        print("# Begin Peak Refinement")

    cluster_work=cluster0

    DoRefine=True
    iter=0
    while (DoRefine):
        iter=iter+1
        if (verbose > 2):
            print("# Iteration {:d}:".format(iter))
            print("# ---------------------------------------")

        new_cluster=[]
        for group in cluster_work:
            xd=numpy.array(group['xd'])
            yd=numpy.array(group['yd'])
            rd=numpy.array(group['rd'])
            rd_med=numpy.median(rd)
            rd_std=numpy.std(rd)
#
            wsm=numpy.where(numpy.logical_and(rd>rd_med-2.0*rd_std,rd<rd_med+2.0*rd_std))
            rd_med=numpy.median(rd[wsm])
            rd_std=numpy.std(rd[wsm])
#
#           Check to see if there is evidence the peak should be split
#
            wsm=numpy.where(numpy.logical_and(rd>rd_med-2.0*rd_std,rd<rd_med+2.0*rd_std))
            lost_member=len(rd)-len(rd[wsm])
            print(" {:d} should lose {:d} ".format(group['cnt'],lost_member))

            wsm=numpy.where(numpy.logical_and(rd>rd_med-2.0*rd_std,rd<rd_med+2.0*rd_std))
            xd_med=numpy.median(xd[wsm]) 
            xd_std=numpy.std(xd[wsm]) 
            yd_med=numpy.median(yd[wsm]) 
            yd_std=numpy.std(yd[wsm]) 
            if (verbose > 2):
                print("# Peak {id:d}({cnt:4d} members): {x0:.3f}+/-{dx0:.4f}, {y0:.3f}+/-{dy0:.4f}".format(
                    id=group['cnt'],
                    cnt=rd[wsm].size,
                    x0=xd_med,dx0=xd_std,
                    y0=yd_med,dy0=yd_std))
            new_cluster.append({'cnt':group['cnt'],'x0':xd_med,'y0':yd_med,'dx0':xd_std,'dy0':yd_std,'n0':rd[wsm].size,'xd':[],'yd':[],'rd':[]})
#
#       Check whether there is evidence that a peak should be split
#
        remove_cluster=[]
        add_cluster=[]
        for i, group in enumerate(new_cluster):
            lost_member=cluster0[i]['n0']-new_cluster[i]['n0'] 
            print(i,cluster0[i]['n0'],new_cluster[i]['n0'],lost_member)
            if (lost_member > 2*dfloor):
                print("# Analysis of Peak {:d} has initiated a search for a subpeak?".format(group['cnt']))
#
#               Redo analysis on subpeak
#
                sub_xdata=cluster0[i]['xd']
                sub_ydata=cluster0[i]['yd']
                SubHist=Form2DHist(sub_xdata,sub_ydata,0.25,verbose=verbose)
                SubCluster=FindClusters(SubHist,3,dfloor,BlankSize=2,verbose=verbose)
#                SubCluster=assign_cluster_members(SubCluster,Data,smatch=None,verbose=verbose)

                if (len(SubCluster) > 1):
                    print("# Identified {:d} distinct peaks".format(len(SubCluster)))
                    remove_cluster.append(group['cnt'])
                    for group in SubCluster:
                        add_cluster.append(group)
#                    sub_icluster=0
                else:
                    print("# Failed to identify more than 1 distinct peak")

        DoOver=False
        if (len(remove_cluster)>0):
            new_new_cluster=[group for group in cluster0 if group['cnt'] not in remove_cluster]
            for i, group in enumerate(new_new_cluster):
                new_new_cluster[i]['xd']=[]
                new_new_cluster[i]['yd']=[]
                new_new_cluster[i]['rd']=[]
        if (len(add_cluster)>0):
            for group in add_cluster:
                new_new_cluster.append(group)
        if (len(remove_cluster)>0):
            for i, group in enumerate(new_new_cluster):
                new_new_cluster[i]['cnt']=i+1
            print("# Restarting Refinement Iteration Sequence")
            cluster0=new_new_cluster
            cluster0=assign_cluster_members(cluster0,Data,smatch=None,verbose=verbose)
            new_cluster=cluster0
            DoOver=True
        else:
            new_cluster=assign_cluster_members(new_cluster,Data,smatch=2.0,verbose=verbose)

        cluster_work=new_cluster
        if (DoOver):
            iter=0
        if (iter > 3):
            DoRefine=False

#
#   Report final parameters (and reform output so elimnate the vectors holding working data.
#
    new_cluster=[]
    if (verbose > 0):        
        print("# Final set of peaks identified")
        print("# ---------------------------------------")
    for group in cluster_work:
        new_cluster.append({'cnt':group['cnt'],'x0':group['x0'],'y0':group['y0'],'dx0':group['dx0'],'dy0':group['dy0'],'n0':group['n0']})
        if (verbose > 0):        
            print("# Peak {id:d}({cnt:4d} members): {x0:.3f}+/-{dx0:.4f}, {y0:.3f}+/-{dy0:.4f}".format(
                id=group['cnt'],
                cnt=group['n0'],
                x0=group['x0'],dx0=group['dx0'],
                y0=group['y0'],dy0=group['dy0']))

    return new_cluster


########################
def which_cluster(x0,y0,Clusters,Sigma=3.0,verbose=0):
    """
    Given a set of clusters, determine which cluster a point is closest.

    inputs:
        x0,y0:      Coordinate value of a point to examine.
        Clusters:   List of dictionaries (row per cluster).  Each dictionary has:
                        'x0','y0':   cluster center
                        'dx0','dy0': cluster width (RMS)
        sig:        How many sigma close must a point be to be considered a cluster member
        verbose:    Provide verbose output (curently there is none).

    OUTPUT: 
        Identifier of cluster.  Returns "None" if no cluster is found.
    """

    dist=numpy.array([math.sqrt((x0-peak['x0'])*(x0-peak['x0'])+(y0-peak['y0'])*(y0-peak['y0'])) for peak in Clusters])
    indhist=numpy.argsort(dist)
    ClusterName=None
    ClusterDist=100000.
    ind=0
    while (ClusterName is None):
        cx=Clusters[indhist[ind]]['x0']
        cy=Clusters[indhist[ind]]['y0']
        cdx=Clusters[indhist[ind]]['dx0']
        cdy=Clusters[indhist[ind]]['dy0']
        cx1=cx-cdx*Sigma
        cx2=cx+cdx*Sigma
        cy1=cy-cdy*Sigma
        cy2=cy+cdy*Sigma
        if ((x0 > cx1)and(x0 < cx2)and(y0 > cy1)and(y0 < cy2)):
            ClusterName='{:d}'.format(Clusters[indhist[ind]]['cnt'])
            ClusterDist=dist[indhist[ind]]
        else:    
            ind=ind+1
            if (ind >= len(Clusters)):
                ClusterName='None'
                ClusterDist=1000000.

    return ClusterName,ClusterDist


##########################################
def find_coadd_attempt(TileName,ReqNum,AttNum,dbh,verbose=0):
    """Get attempt id based on other information about a run
    """

    AttVal=None
    curDB=dbh.cursor()
    query="""SELECT av.pfw_attempt_id as pfw_attempt_id
            FROM {schema:s}pfw_attempt a, {schema:s}pfw_attempt_val av
            WHERE av.key='tilename'
                and av.val='{Tile:s}'
                and av.pfw_attempt_id=a.id
                and a.reqnum={Rnum:s}
                and a.attnum={Anum:s}
        """.format(schema=dbSchema,Tile=TileName,Rnum=ReqNum,Anum=AttNum)

    if (verbose > 0):
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        AttVal=rowd['pfw_attempt_id']

    if (AttVal is None):
        print("Failed to find a PFW_ATTEMPT_ID")
        print("Searching to find list of possible processing attempts for this tile")

        query="""SELECT av.pfw_attempt_id as pfw_attempt_id,
                av.val as tilename,
                a.reqnum as reqnum,
                a.attnum as attnum
            FROM {schema:s}pfw_attempt a, {schema:s}pfw_attempt_val av
            WHERE av.key='tilename'
                and av.val='{Tile:s}'
                and av.pfw_attempt_id=a.id
                order by a.reqnum,a.attnum
            """.format(schema=dbSchema,Tile=TileName)

        if (verbose > 0):
            if (verbose == 1):
                print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
            if (verbose > 1):
                print("# sql = {:s}".format(query))
        curDB.execute(query)
        desc = [d[0].lower() for d in curDB.description]

        print(' ')
        print('             PFW_Attempt Request Attempt')
        print('  TileName      ID        Number  Number')
        print('----------------------------------------')
        count=0
        for row in curDB:
            count=count+1
            rowd = dict(zip(desc, row))
            print('{Tile:12s} {AttID:14d} {Rnum:5d} {Anum:02d}'.format(
                Tile=rowd['tilename'],AttID=rowd['pfw_attempt_id'],Rnum=rowd['reqnum'],Anum=rowd['attnum']))
        if (count<1):
            print("No attempts found")

        print("Aborting")
        exit(1)

    curDB.close()

    return AttVal


##########################################
def get_astromqa_vals(MetaDataTable,FileType,QATable,Columns,attempt,OptConstraint,dbh,dbSchema,verbose=0):
    """Get column values from COADD_EXPOSURE_ASTROM_QA for an attempt
    """

    curDB=dbh.cursor()

    if (OptConstraint is None):
        constraint=""
    else:
        constraint=OptConstraint

    if (Columns is None):
        colnames=""
    else:
        colnames=",{:s}".format(Columns)

    query="""SELECT c.expnum,c.band,c.nite,oe.name as epoch,q.as_contrast,q.xy_contrast,q.ndetect,q.int_ndeg_highsn,q.int_chi2,q.dx,q.dy,q.ref_ndeg_highsn{cnames:s}
            from {schema:s}{metaname:s} m, {schema:s}{qtabname} q, {schema:s}catalog c, {schema:s}ops_epoch oe
            where m.pfw_attempt_id={atval:d} 
                and m.filetype='{ftype:s}'
                and m.filename=q.filename {conval:s}
                and c.pfw_attempt_id=m.pfw_attempt_id
                and c.filename=q.cat_filename
                and c.expnum >= oe.minexpnum and c.expnum <=oe.maxexpnum
            order by c.band,c.expnum
        """.format(cnames=colnames,schema=dbSchema,metaname=MetaDataTable,ftype=FileType,qtabname=QATable,atval=attempt,conval=constraint)

    if (verbose > 0):
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

#    printer.pprint(desc)

    data=[]
    for row in curDB:
#        rowd = dict(zip(desc, row))
        data.append(row)

    if (verbose > 2):
        munger=rm.colMunger(data,desc)
#        munger.replace(lambda x : "run/abt" if type(x) == type(None) else x, "status")
        pdata=munger.get_data()
        pdata = [desc] + pdata
        printer = rp.prettyPrinter()
        printer.pprint(pdata)

    curDB.close()

    return (data,desc)


##########################################
def get_run_info(AttVal,dbh,dbSchema,verbose=0):
    """Get metadata for an attempt
    """

    curDB=dbh.cursor()
    query="""SELECT a.reqnum as reqnum,
                a.attnum as attnum,
                av.val as tilename
            from {schema:s}pfw_attempt_val av, {schema:s}pfw_attempt a
            where a.id={atval:d} 
                and av.pfw_attempt_id=a.id
                and av.key='tilename'
        """.format(schema=dbSchema,atval=AttVal)

    if (verbose > 0):
        if (verbose == 1):
            print("# sql = {:s} ".format(" ".join([d.strip() for d in query.split('\n')])))
        if (verbose > 1):
            print("# sql = {:s}".format(query))
    curDB.execute(query)
    desc = [d[0].lower() for d in curDB.description]

    for row in curDB:
        rowd = dict(zip(desc, row))
        metadata=rowd

    curDB.close()

    return metadata


##########################################
if __name__ == "__main__":

    import argparse
    import os
    import stat
#    import re
    import sys
    import time
#    from datetime import datetime
    import math
    import numpy
    import despydb.desdbi 
    import opstoolkit.reportquery  as rq
    import opstoolkit.reportmunger as rm
    import opstoolkit.reportprint  as rp

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Ellipse

    svnid="$Id: coadd_astrorefine_qa.py 44367 2016-10-11 14:11:26Z rgruendl $"

    parser = argparse.ArgumentParser(description='Investigate properties of files within archive for a specific PROCTAG')
#    parser.add_argument('-t','--table',    action='store', type=str, default='COADD_EXPOSURE_ASTROM_QA', help='Table to probe')
    parser.add_argument('-a','--attempt',  action='store', type=str, default=None, help='Attempt ID to probe')
    parser.add_argument('--tilename',      action='store', type=str, default=None,  help='TILENAME to query  for results')
    parser.add_argument('--reqnum',        action='store', type=str, default=None,  help='REQNUM to query for results')
    parser.add_argument('--attnum',        action='store', type=str, default=None,  help='ATTNUM to query for results')
#    parser.add_argument('-f','--filetype', action='store', type=str, default='coadd_xml_scamp',  help='Filetype to probe')
#    parser.add_argument('-m','--metadata', action='store', type=str, default='MISCFILE',  help='Metadata table where filetype resides (default=MISCFILE)')
    parser.add_argument('-c','--constraint',action='store', type=str, default=None, help='Additional constraint')
    parser.add_argument('-C','--Columns',   action='store', type=str, default=None, help='Additional  Columns')
    parser.add_argument('-q', '--qaplot',   action='store', type=str, default=None,  help='Spawns qaplot where filename is given as argument')
    parser.add_argument('-s', '--section',  action='store', type=str, default=None,  help='section of .desservices file with connection info')
    parser.add_argument('-S', '--Schema',   action='store', type=str, default=None,  help='DB schema (do not include \'.\').')
    parser.add_argument('-v', '--verbose',  action='store', type=int, default=0,     help='Print extra (debug) messages to stdout')
    args = parser.parse_args()
    if (args.verbose):
        print("Args: ",args)
    verbose=args.verbose
#
#   Check for minimal input (attempt or TileName+Reqnum)
#
    find_attempt=False
    if (args.attempt is None):
        find_attempt=True
        if ((args.tilename is None)or(args.reqnum is None)or(args.attnum is None)):
            print("Insufficient constraint to isolate an attempt")
            print("Must specify either --attempt  or --tilename, --reqnum, and --attnum")
            print("Aborting")
            exit(1)
#
#   Check for user specified schema
#
    if (args.Schema is None):
        dbSchema=""
    else:
        dbSchema="%s." % (args.Schema)

#
#   Setup DB connection.
#
    try:
        desdmfile = os.environ["des_services"]
    except KeyError:
        desdmfile = None
    dbh = despydb.desdbi.DesDbi(desdmfile,args.section,retry=True)
#    cur = dbh.cursor()

    if (find_attempt):
        AttVal=find_coadd_attempt(args.tilename,args.reqnum,args.attnum,dbh,verbose)
    else:
        AttVal=int(args.attempt)

################################################################################################
    FileType='coadd_xml_scamp'
    MetaDataTable='MISCFILE'
    DataTable='COADD_EXPOSURE_ASTROM_QA'
   
    RunData=get_run_info(AttVal,dbh,dbSchema,verbose)
 
    retdata=get_astromqa_vals(MetaDataTable,FileType,DataTable,args.Columns,AttVal,args.constraint,dbh,dbSchema,verbose)

    data=retdata[0]
    desc=retdata[1] 

#    colstr=" ".join(args.Columns.split(","))
#    print colstr

    pdata=[]
    for row in data:
        rowd = dict(zip(desc, row))
        pdata.append({'x':3600.*rowd['dx'],'y':3600.*rowd['dy'],'band':rowd['band']})
#    clusters=find_clusters(pdata,dfloor=3.,ncluster=2,verbose=verbose)
    clusters=find_clusters2(pdata,dfloor=3.,ncluster=4,verbose=verbose)

    lastband='RAG'
    print("# ")
    print("# ")
    print("#                     int_ndeg    int      dx      dy    ref_ndeg     Member  ")
    print("# expnum band ndetect  highsn     chi2   [asec]  [asec]   highsn    of Cluster")
#    print desc
    for row in data:
        rowd = dict(zip(desc, row))
        if (rowd['band']!=lastband):
            lastband=rowd['band']
            print("#-----------------------------------------------------------------------------------")
       
        ClusterID,ClusterDist=which_cluster(3600.*rowd['dx'],3600.*rowd['dy'],clusters,Sigma=10.0,verbose=verbose)
    
        if((ClusterID == "None")or(ClusterDist > 4.0)):
            expflag='FLAG'
        else:
            expflag=' '

        if (rowd['xy_contrast'] < 15):
            expflag='{:s}cFLAG'.format(expflag)
#        print rowd['xy_contrast'],ClusterID,ClusterDist

        print(" {enum:7d} {band:5s} {ndet:7d} {i_nd_hsn:7d} {i_chi2:8.2f} {offx:8.2f} {offy:8.2f} {r_nd:7d} {cid:s} {epoch:6s} {eflag:4s}".format(
            enum=rowd['expnum'],
            band=rowd['band'],
            ndet=rowd['ndetect'],
            i_nd_hsn=rowd['int_ndeg_highsn'],
            i_chi2=rowd['int_chi2'],
            offx=3600.*rowd['dx'],
            offy=3600.*rowd['dy'],
            r_nd=rowd['ref_ndeg_highsn'],
            cid=ClusterID, 
            epoch=rowd['epoch'],
            eflag=expflag)
        )
#    pdata=[]
#    for row in data:
#        rowd = dict(zip(desc, row))
#        pdata.append({'x':3600.*rowd['dx'],'y':3600.*rowd['dy'],'band':rowd['band']})
#    clusters=find_clusters(pdata,dfloor=3.,ncluster=2,verbose=verbose)
    if (args.qaplot is not None):
        PlotFileName='{:s}.png'.format(args.qaplot)
        PlotTitle='{tname:s} (reqnum={rnum:d}, attnum={anum:02d})'.format(tname=RunData['tilename'],rnum=RunData['reqnum'],anum=RunData['attnum'])
        QAplt_astrom_group(PlotFileName,PlotTitle,pdata,clusters,args.verbose)


    exit(0)
