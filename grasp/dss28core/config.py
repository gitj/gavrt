"""
:mod:`dss28core.config`
-----------------------

configuration infomation

Provides:

* :attr:`serialPortMap` : Dictionary mapping devices to serial ports (/dev/ttyUSB*)
"""


import utils
from loggers import corelog
import os
DSS28_DIR = '/home/dss28' 

bitfiles_dir = os.path.join(DSS28_DIR,'bitfiles')
try:
    serialPortMap = utils.unpickle(os.path.join(DSS28_DIR,'serialPorts.pkl'))
except Exception, e:
    corelog.exception("Failed to load serialPorts.pkl configuration")
    serialPortMap = dict()    
