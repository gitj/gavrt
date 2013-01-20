from tables import *
import numpy as np
import personalities
import gavrtdb
import time
import Pyro.core
from gluon.storage import Storage
from gavrt_constants import ibob_fiber_map
from rss import channelInfo


class DataInterface(Pyro.core.ObjBase):
    def __init__(self,h5='/tmp/rt%d.h5',ibobs=range(8)):
        self.running = True
        Pyro.core.ObjBase.__init__(self)
        self._h5 = h5
        self._ibobs = ibobs
        self._gdb = gavrtdb.GavrtDB()
        self._pdict = self._gdb.getPersonalities()  # we assume no new personalities are added while running
        self._setupPersonalities()
        self._bb = [None]*8
        self._rf = [None]*8
        self._ch = [None]*8
        self._f0 = [None]*8
        self._updateBBRF()

        self.Data = Storage
    def ping(self):
        return True
    def getData(self,bbrf=True):
#        print "getData"
        d = self.Data()

        for ib in self._ibobs:
            ibc = self.Data()
            setattr(d,'ib%d'%ib,ibc)
            h5 = None
            try:
                h5 = openFile(self._h5 % ib,'r')
            
                for meas in h5.root:
                    name = meas._v_name
                    #print "meas",name
                    if name in ['InfoTable']:
                        ibc.InfoTable = meas[:]
                        for key in self._personalities[ib]._controlRegisters:
                            name = key.replace('/','_')
                            try:
                                self._personalities[ib]._controlRegisters[key] = ibc.InfoTable[-1][name]
                            except Exception, e:
                                #print "could not update",key,e
                                pass
                        continue
                    if name in ['file_info']:
                        ibc.file_info = meas[:]
                        ibc.personality = ibc.file_info[0]['personality'] 
                        continue
                    mc = self.Data()
                    shortname = ''
                    for k in name:
                        if k == k.upper():
                            shortname += k
                    ibc.__setattr__(shortname,mc)
                    idx = meas.index[0]
                    table = meas.table[:]
                    for k in table.dtype.fields.keys():
                    #    print "key",k
                        mc.__setattr__(k,table[k])
                    for arry in meas:
                        
                        name = arry._v_name
                    #    print "arry",name
                        if name in ['index','table']:
                            continue
                        a = arry[:]
                        alen = a.shape[0]
                        fixarry = np.empty_like(a)
                        fixarry[:(alen-idx)] = a[idx:]
                        fixarry[(alen-idx):] = a[:idx]
                        mc.__setattr__(name,fixarry)
            except IOError, e:
                pass
            
            
            finally:
                if h5 is not None:
                    h5.close()
                    
            #bb,rf = self._calcBBRF(ib)
            if time.time() - self._lastupdate > 10 and bbrf:
                self._updateBBRF()

            setattr(d,'bb%d' % ib, self._bb[ib])
            setattr(d,'rf%d' % ib, self._rf[ib])
#        print "returning d"
        return d
    
    def _setupPersonalities(self):
        self.spss = self._gdb.getSPSSStatus()
        self._personalities = {}
        self._dsgnids = {}                          #TODO Later use this to chekc if an ibob has been reprogrammed
        todrop = []
        for ib in self._ibobs:
            clk = float(self.spss['iBOBADCClock%d' % ib])
            dsgnid = self.spss['iBOBDesignID%d' %ib]
            if dsgnid == 1:
                todrop.append(ib)
            else:
                self._dsgnids[ib] = dsgnid
                name = self._pdict[dsgnid]
                p = getattr(personalities, name)
                p = p(None,clk)
                self._personalities[ib] = p
        for k in todrop:
            self._ibobs.remove(k)
        
    
    def _calcBBRF(self):
        self.rss = self._gdb.getRSSStatus()
        self.spss = self._gdb.getSPSSStatus()
        bbrf = {}
        for ib in self._ibobs:
            fib = ibob_fiber_map[ib]
            chan = self.rss['Fiber%d' % fib]
            self._ch[ib] = chan
            rx,pol,sb = channelInfo(chan)
            
            
            syn = float(self.rss['RX%d_Synth' % rx])
            
            f0 = syn*4 - 22000
            self._f0[ib] = f0
            
            p = self._personalities[ib]
            bb = p._bbfrq()
            clk = p._adcClock
            if clk < 200:
                bb += 2*clk  #if clk is 128, we want bb to go from 256 MHz to 384 MHz
            
            rf = f0 + sb*bb
            bbrf[ib] = (bb,rf)
        
        return bbrf
    
    def _updateBBRF(self):
        bbrf = self._calcBBRF()
        for ib in self._ibobs:
            
            self._bb[ib] =bbrf[ib][0]
            self._rf[ib] =bbrf[ib][1]
            
        self._lastupdate = time.time()
    def quit(self):
        self.running = False
if __name__=="__main__":
    print "Starting Data Interface server"
    import Pyro.naming
#    Pyro.config.PYRO_MULTITHREADED = 0
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('DataInterface')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    di = DataInterface()
    daemon.connect(di,"DataInterface")

    lastt = time.time()
    while di.running:
        #print "handle req:"
        daemon.handleRequests(1) #1 second timeout