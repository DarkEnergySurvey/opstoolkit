
class colMunger(object):
    def __init__(self, data, header):
        self._assert_uniform(data)
        self.cols = zip(*data)
        self.header= [h.upper() for h in header]

    def _assert_uniform(self, data):
        """ assert all rows are the same lenght """
        #nothing to chedk if data are null
        if not data : return  
        rowlengths = set([len(r) for r in data])
        assert len(rowlengths)==1

    def index_from_header(self,heading):
        """ return column index from heading""" 
        index = self.header.index(heading.upper())
        return index

    def replace(self, func, heading):
        """ replace data in column named by heading with  results of func 
        
        The input to func is the datum to be replaced
        """
        #return on empty data
        if not  self.cols : return
        index = self.index_from_header(heading)
        new = [func(item) for item in self.cols[index]]
        self.cols[index] = new 


    def get_data(self):
        """ return edited 2-d array"""
        return zip(*self.cols)

    def get_header(self):
        """ return canonicalized header"""
        return self.header

    def get_optimal_floatformat(self, heading):
        """ return the optimal floatformat for python
             
            the idea is to make columns of data
            taht information is conveyed but are 
            of minimum width, were the min width
            is not stable.  For example data rates.
            
        """
        import itertools
        import math
        #import pdb
        #pdb.set_trace()
        index = self.index_from_header(heading)
        col = self.cols[index]
        # take only the floats -- DB queries can return none,
        col = itertools.ifilter(lambda x : type(x) == type(1.0), col)
        col = [c for c in col] #intertools returns an interator
        #consider the decimal point and the integer part of the floats
        largest = max(col)
        least   = min(col)
        if least >= 0 : negative_places = 0
        else: negative_places = 1
        largest = max(abs(largest), abs(least),1.0)
        int_places = math.ceil(math.log(largest,10))
        #if all data fractional, keep one place, deal wiht neg numebrs
        int_places = max(1, int_places)
        int_places = int(int_places) + negative_places


        #consider the positive definite fractional part
        fractions = [abs(x)-int(abs(x)) for x in col]
        least   = min(fractions)
        #handle corner case where least = 0.0
        if least == 0.0:
            fract_places = 0
        else:
            # seed with enough places to show one digit of the 
            # smallest
            fract_places = int(math.ceil(abs(math.log(least,10))))
        fract_places += 1  # add one more to show variation
        # note format syntax is to escape {  with doubling {{
        return "{{:>0{:d}.{:d}f}}".format(int_places,fract_places)


if __name__ == "__main__":
    import math
    #
    #  tests of optimal format
    #
    header = ["pi", "eps","tenth", "one","big","zed"]
    data=[
        [math.pi, -.0001235, 0.1, 1.0, 12345.2, 0.0],
        ]
    m = colMunger(data, header)
    for h in header:
        fmt = m.get_optimal_floatformat(h)
        print "heading, format:", h, fmt
        col = zip(*data)[m.index_from_header(h)]
        for c in col :
            print fmt.format(c),
        print

    #
    #  Test of type sentiitve changes to a col
    #
    def f(x):
        print "in f", x
        return "dog"

    header = ["mixedtype"]
    data=[
        [1], [-.0001235], [None], ["text"], [{"dict":None}],
        ]
    m=colMunger(data,header)
    print data
    m.replace(lambda x : "n/a" if x == None  else x, "mixedtype")
    #m.apply(f, "mixedtype")
    print m.get_data()
