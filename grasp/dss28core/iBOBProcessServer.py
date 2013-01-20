"""
:mod:`dss28core.iBOBProcessServer`
----------------------------------

Stand alone program to run in the background which starts and stops iBOB data capture/interface programs when iBOBs are programmed

Available as Pyro object **ProcessServer**
"""

import numpy as np
import sys, time,os
from multiprocessing import Process
import stat

import Pyro.naming
import Pyro.core

import gavrtdb

from IbobServer import IbobServer
import personalities

from loggers import corelog


class iBOBProcessServer(Pyro.core.ObjBase):
    def __init__(self,sql = True):
        Pyro.core.ObjBase.__init__(self)
        if sql:
            self.sql = True
            corelog.info("Connecting to GavrtDB")
            self.gdb = gavrtdb.GavrtDB(rw=True)
        else:
            self.sql = False
            self.gdb = None
            
        self.iBOBProcesses = {}
        self.iBOBServers = {}
        self.iBOBProxies = {}
        
    def ping(self):
        return True
    def _startIbobServers(self,ibs):
        ns = Pyro.naming.NameServerLocator().getNS()

        for ib in ibs:
            if self.iBOBProcesses.has_key(ib):
                self._stopIbobServer(ib)
            thisServer = IbobServer(ib)
            self.iBOBServers[ib] = thisServer
            thisProcess = Process(target = thisServer.run, args = ())
            self.iBOBProcesses[ib] = thisProcess
            thisProcess.start()
            corelog.info("Started server for iBOB %d" % ib)
        corelog.info("Sleeping for 5 seconds while iBOB processes initialize...")
        time.sleep(5)
        corelog.debug("Done sleeping, starting to resolve ibobs")
        for ib in ibs:
            thisiBOB = ns.resolve(':IBOB.%d'%ib).getProxy()
            tupd,pers,clk = self.gdb.getPersonality(ib)
            corelog.info("Database entry at %s indicates iBOB %d, personality: %s clock rate %f MHz" % (time.ctime(tupd),ib,pers,clk))
            thisiBOB.set_personality(getattr(personalities,pers))
            self.iBOBProxies[ib] = thisiBOB
            corelog.info("Set personality for ibob %d" % ib)

    def _stopIbobServer(self,ib):
        corelog.info("Stopping IBOB %d server" % ib)
        ns = Pyro.naming.NameServerLocator().getNS()
        thisiBOB = ns.resolve(':IBOB.%d'%ib).getProxy()
        try:
            thisiBOB.quit()
        except:
            corelog.warning("Couldn't quit iBOB %d server nicely, so terminating" % ib)
        self.iBOBProcesses[ib].terminate()
        if self.iBOBProcesses.has_key(ib):
            del self.iBOBProcesses[ib]
        if self.iBOBServers.has_key(ib):
            del self.iBOBServers[ib]  
            
    def _startRecord(self,ibobids,datapath):
        corelog.info("Got request to record from ibobs: %s In path: %s" % (str(ibobids),datapath))
        
        try:
            os.mkdir(datapath)
            os.chmod(datapath,stat.S_IRWXO | stat.S_IRWXG | stat.S_IRWXU)
        except:
            corelog.exception("Couldn't create data path %s" % datapath)
        for id,ibob in self.iBOBProxies.items():
            if id in ibobids:
                ibob.start_writing(os.path.join(datapath,'ibob%d.h5' % id))
        

if __name__=="__main__":
    import Pyro
    import ifconfig
    for k in range(4):
        cfg = ifconfig.ifconfig('eth%d'%k)
        if cfg['addr'].startswith('192.168'):
            myip = cfg['addr']
            break
    
    print "My IP is:",myip
    Pyro.config.PYRO_HOST = myip
    corelog.info("Starting iBOBProcessServer on %s" % myip)

    Pyro.core.initServer()
    Pyro.config.PYRO_MULTITHREADED = 0
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('ProcessServer')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    daemon.connect(iBOBProcessServer(),"ProcessServer")
    daemon.requestLoop()        