"""
:mod:`dss28core.loggers`
------------------------

Provides easy access to logging facilities. Logging is configured using '$DSS28/logging.conf'

Provies the following loggers:

* corelog : Logging for dss28core modules
"""


import logging.config
import cloghandler
import os
try:
    DSS28 = os.environ['DSS28']
except:
    DSS28 = os.path.join(os.environ['HOME'],'dss28')
    if not os.path.exists(DSS28):
        raise Exception("Please define $DSS28 or create $HOME/dss28 and $HOME/dss28/logs")
logging.config.fileConfig(os.path.join(DSS28,'logging.conf'))
corelog = logging.getLogger('dss28core')