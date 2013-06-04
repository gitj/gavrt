"""
:mod:`dss28core.config`
-----------------------

configuration infomation

Provides:

* :attr:`serialPortMap` : Dictionary mapping devices to serial ports (/dev/ttyUSB*)
"""


import grasp.utils as utils
from loggers import corelog
import os
try:
    DSS28_DIR = os.environ['DSS28_DIR'] #'/home/dss28' 
except:
    raise Exception("DSS28_DIR environment variable must be defined!")

bitfiles_dir = os.path.join(DSS28_DIR,'bitfiles')
try:
    serialPortMap = utils.unpickle(os.path.join(DSS28_DIR,'serialPorts.pkl'))
except Exception, e:
    corelog.exception("Failed to load serialPorts.pkl configuration")
    serialPortMap = dict()    
