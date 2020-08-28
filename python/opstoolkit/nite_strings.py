def increment_nite(night):
    """
        Increment a night string YYYYMMDD by one day
    """
    year=int(night[0:4])
    if ((year%4 == 0)and(year%100 != 0)):
        mday=[31,29,31,30,31,30,31,31,30,31,30,31]
    else:
        mday=[31,28,31,30,31,30,31,31,30,31,30,31]
    month=int(night[4:6])
    day=int(night[6:8])
    day=day+1
    if (day>mday[month-1]):
        day=1
        month=month+1
        if (month > 12):
            month=1
            year=year+1
    syear='%04d' % year
    smonth='%02d' % month
    sday='%02d' % day
    new_night=syear+smonth+sday
    return new_night

def decrement_nite(night):
    """
        Decrement a night string YYYYMMDD by one day
    """
#   import numpy as np
    year=int(night[0:4])
    if ((year%4 == 0)and(year%100 != 0)):
        mday=[31,29,31,30,31,30,31,31,30,31,30,31]
    else:
        mday=[31,28,31,30,31,30,31,31,30,31,30,31]
    month=int(night[4:6])
    day=int(night[6:8])
    day=day-1
    if (day<1):
        month=month-1
        if (month < 1):
            month=12
            year=year-1
            day=mday[month-1]
    syear='%04d' % year
    smonth='%02d' % month
    sday='%02d' % day
    new_night=syear+smonth+sday
    return new_night

