"""
:mod:`utils`
------------

General purpose utilities
"""

import cPickle

def unpickle(fname):
    fh = open(fname,'r')
    res = cPickle.load(fh)
    fh.close()
    return res

def pickle(obj,fname,protocol=2):
    fh = open(fname,'w')
    cPickle.dump(obj,fh,protocol=protocol)
    fh.close()
