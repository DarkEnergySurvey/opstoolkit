   
class  prettyPrinter(object):
   """
   print the data in columns of uniform width.
   
   the columns widths are automatically built   
   use:
   prettyPrinter().pprint(data)

   defaultt formatting is used, and can be adjusted
   by setting type-specific formats. Ttwo API's exist
   formats can be anyting acceptable to
   format e.f {:f3.2} for float
   or a function rendering the type into a string.

   default formate are used for all types except for
   datetime deltatime and lists and tuples.

   """
   def __init__(self):
      import datetime
      self.fmatdict={}

      #default formats
      # datetimes --no usconds for datetimes (returned by Oracle, for example)
      self.set_format_by_type(datetime.datetime.today(), "{:%x %X}")


      #default functions are needed for more complesx type.
      #timedeltas -- no usecs and no days, just more than 24 hours for timedeltas
      # sets -- default is narrow, one set member and elipses  set has more than one...
      # lists, typle -- render according to primitive type, Join elements with comma
      td = lambda d : "{}:{}:{}".format(d.seconds/3600 + d.days*24,(d.seconds/60)%60,d.seconds%60)
      self.set_render_func_by_type(datetime.timedelta(1,1), td)
      self.set_render_func_by_type(set([1,2]), format_set)
      self.set_render_func_by_type([],self._render_list_to_string)
      self.set_render_func_by_type((),self._render_list_to_string)
      self.set_render_func_by_type({},self._render_dict_to_string)
      
   def set_format_by_type(self, value, format):
      """ set format for a types of which value is an instance 
            e.g ps.et_format(type(1), '{:<5d}'

            iplemented by passinf the associated format function
            to the function variant of the...
       """
      self.set_render_func_by_type(value, format.format)
   def set_render_func_by_type(self, value, function):
       """ set a function that will render a type 

           provides a function to format a thing of the given type 
       """
       self.fmatdict[type(value)] = function
   def _render_thing(self, thing):
      """ return an ascii value based on type

           rendering turns a python variable into a string.
           using a format which can be specific to its type

      """
      function = "{:}".format
      if (type(thing) in self.fmatdict):
         function  = self.fmatdict[type(thing)]
      return function(thing).strip()

   def _render(self, data):
      rendered=[]
      for row in data:
         rendered.append(self._render_row(row))
      return rendered


   def _render_row(self, alist):
      """ produce a list of ll the things in a list, rendered

          deta item that is itsells a list
          we want to amke sure we can render lists of dates
          acording to the format rules
      """
      return  [self._render_thing(item) for item in alist]

   def _width(self, array):
      """ return the width of the largest ascii element in  array"""
      return  max([len(self._render(c)) for c in array ])
                     
   def _fmats(self, rdata):
      """ calculate the formats  to apply to  a row in the array
      
      This format is inteneded not to convert a type to ascii,
       but to blank pad an ascii type to produce a veritbal column
       """

      widths = [self._width(col) for col in list(zip(*rdata))]
      fmats = ' '.join(['{:>%d}' % width for width in widths ])
      return fmats

   def _assert(self, data):
      """ 
      ensure that data is per contract
      thsi has been a bug-abo in the pars

      -- all rowa are of equal length
      """
      lengths = [len(r) for r in data]
      least = min (lengths)
      most  = max (lengths)
      assert least == most

#############################################
#
#  Render complex types the user might present in
#  in a string.
#
#############################################

   def _render_list_to_string(self, alist):
      """  
          render all the things in a 1-d list into a string
          used as the default renderer for a list type.


      """
      return  ",".join(self._render_row(alist))


   def _render_dict_to_string(self, adict):
      """  
          render all the things in adictionary to a string.
      """
      alist = [ "%s:%s" % (self._render_thing(k), 
                           self._render_thing(adict[k])
                           )  for k in adict.keys()]
      return  ",".join(self._render_row(alist))


   def pprint(self, data):
      """ prettytprint a 2-D array of uniform row lenght to sdtout"""
      self._assert(data)
      data = self._render(data)  # make elements ascii
      fmats = self._fmats(data)    # get array of padding formats)
      for row in data:
         print(fmats.format(*row))

   def csvprint(self, data):
      """ write a CSV file to stdout"""
      import csv
      import sys
      # self._assert(data)  CSV data  row lenght can vary
      data = self._render(data)  # make elements ascii
      writer = csv.writer(sys.stdout, delimiter=',',
                          quotechar='"', 
                          quoting=csv.QUOTE_MINIMAL,
                          lineterminator="\n")
      for row in data: writer.writerow(row)


########################################################
#
#  foratting support functions
#
########################################################

#
#  support for pretty printing sets.
#  
def format_set_wide(set):
    formatSet(set, isWide=True)
def format_set(set, isWide=False):
    """render a set into a printable string

    if we are 'wide': then return a blank-seperated
    set elemements, if we are narrow return a 
    member of the set, indicate that there  is more 
    than one element of a set by appending elipsis...

    """
    if isWide:
        list = [s for s in set]
        ret = ";".join(list)
        return ret
    else:
        if len(set) == 0:
            return ""
        elif len(set) == 1:
            return "%s" % iter(set).next()
        else:
            return "%s..." % iter(set).next()


def create_run_str(reqnum, unitname, attnum):
    """ Create the standard string identifying a run to a human"""
    run = '%s_r%sp%02d' % (unitname, reqnum, int(attnum))
    return run

def unique_strings(l):
   dict = {}
   for s in l:
      dict[s] = 1
   return dict.keys()

def main(args):
   """ tests"""
   p = prettyPrinter()
   p.pprint ([
               ["xval", "yval"],
               [datetime.timedelta(1,12398) ,datetime.datetime.today()],
               [1.2, [1,2,3]],
               [set(["dog", "cat", "Dog"]), set(["dog"])],
               [ [ ["row1"],["row2a","row2b"]], {1:2, "dog":datetime.datetime.today()}]
             ])


if __name__ == "__main__":

   import os
   import sys
   import time
   import datetime
   import argparse 

   """Create command line arguments"""
   parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
   parser.add_argument('--debug','-d',help='print debug info', default=False, action='store_true')
   parser.add_argument('--csv','-c',help='print as a CSV (def pretty print)', default=False, action='store_true')
   parser.add_argument('--days_back','-b',help='how far to got back in time(days)', default=1.0)
        
   args = parser.parse_args()

   main(args)

