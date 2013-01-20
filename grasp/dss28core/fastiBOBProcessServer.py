import numpy as np
import sys, time,os
from multiprocessing import Process
import stat

import Pyro.naming
import Pyro.core


from fastibobcapture import FastIbobCapture

import ifconfig
for k in range(4):
    cfg = ifconfig.ifconfig('eth%d'%k)
    if cfg['addr'].startswith('192.168'):
        myip = cfg['addr']
        break

print "My IP is:",myip
Pyro.config.PYRO_HOST = myip

class FastIbobProcessServer(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)            
        self.ibobProcesses = {}
        self.ibobServers = {}
        self.ibobProxies = {}
        
    def _startIbobServers(self,ibs):
        ns = Pyro.naming.NameServerLocator().getNS()

        for ib in ibs:
            if self.ibobProcesses.has_key(ib):
                self._stopIBOBServer(ib)
            thisServer = FastIbobCapture(ib)
            self.ibobServers[ib] = thisServer
            thisProcess = Process(target = thisServer.run, args = ())
            self.ibobProcesses[ib] = thisProcess
            thisProcess.start()
        print "waiting 5 seconds.."
        time.sleep(5)
        for ib in ibs:
            thisibob = ns.resolve(':fastIBOB.%d'%ib).getProxy()
            self.ibobProxies[ib] = thisibob

    def _stopIBOBServer(self,ib):
        print "stopping ibob",ib
        ns = Pyro.naming.NameServerLocator().getNS()
        thisibob = ns.resolve(':fastIBOB.%d'%ib).getProxy()
        try:
            thisibob.quit()
        except:
            print "couldn't quit, terminating"
        self.ibobProcesses[ib].terminate()
        if self.ibobProcesses.has_key(ib):
            del self.ibobProcesses[ib]
        if self.ibobServers.has_key(ib):
            del self.ibobServers[ib]  
            
    def _startRecord(self,ibobids,datapath):
        print "Got request to record from ibobs:",ibobids,"In path:",datapath
        
        os.mkdir(datapath)
        os.chmod(datapath,stat.S_IRWXO | stat.S_IRWXG | stat.S_IRWXU)
        for id,ibob in self.ibobProxies.items():
            if id in ibobids:
                ibob.start_writing(datapath)
                
    def _stopRecord(self):
        for ibob in self.ibobProxies.values():
            ibob.stop_writing()
        

if __name__=="__main__":
    
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('FastProcessServer')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    daemon.connect(FastIbobProcessServer(),"FastProcessServer")
    daemon.requestLoop()        