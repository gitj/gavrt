"""
:mod:`dss28core.valonServer`
----------------------------

Interface to Valon Synthesizer which provides ADC sampling clock for the SPSS ROACH.

This should be automatically run as a stand-alone program to provide access to the synthesizer through Pyro.

Todo:

* Currently does not log synthesier state in the GavrtDB.
"""
import Pyro.core
import gavrtdb
import config
from loggers import corelog
from valon_synth import Synthesizer, SYNTH_A, SYNTH_B

class ValonServer(Pyro.core.ObjBase, Synthesizer):
    """
    Attempts to connect to serial port defined in 
    """
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
        try:
            Synthesizer.__init__(self,config.serialPortMap['Valon'])
        except:
            corelog.exception("Couldn't open Valon serial port")
            return
        self.conn.setTimeout(1)
        self.setup()
        
    def setLevelA(self,level=-1):
        try:
            r = self.set_rf_level(SYNTH_A,level)
        except:
            corelog.exception("Couldn't set RF level %f" % level)
            return False
        if r:
            corelog.info("Set level to %d",level)
        else:
            corelog.warning("Set level reply was NOACK")
        return r

    def setLevelB(self,level=-1):
        try:
            r = self.set_rf_level(SYNTH_B,level)
        except:
            corelog.exception("Couldn't set RF level %f" % level)
            return False
        if r:
            corelog.info("Set level to %d",level)
        else:
            corelog.warning("Set level reply was NOACK")
        return r
    
    def setFreqA(self,freq,chan_spacing=1):
        try:
            r = self.set_frequency(SYNTH_A,freq,chan_spacing=chan_spacing)
        except:
            corelog.exception("Couldn't set frequency %f" % freq)
            return False
        if r:
            corelog.info("Set frequency to %f",freq)
        else:
            corelog.warning("Set frequency reply was NOACK")
        return r

    def setFreqB(self,freq,chan_spacing=1):
        try:
            r = self.set_frequency(SYNTH_B,freq,chan_spacing=chan_spacing)
        except:
            corelog.exception("Couldn't set frequency %f" % freq)
            return False
        if r:
            corelog.info("Set frequency to %f",freq)
        else:
            corelog.warning("Set frequency reply was NOACK")
        return r
    def setFreq(self,freq):
        self.setFreqA(freq)
        self.setFreqB(freq)
        return self.lockStatus()
    def lockStatus(self):
        try:
            a = self.get_phase_lock(SYNTH_A)
            b = self.get_phase_lock(SYNTH_B)
        except:
            corelog.exception("Couldn't get lock status")
        if not (a and b):
            corelog.warning("Valon not in lock - A: %s  B: %s" %(str(a),str(b)))
        else:
            corelog.info("Valon synthesizer is in lock")
        return (a,b)
    def setup(self):
        self.setLevelA(-1)
        self.setLevelB(-1)
        self.setFreqA(1024)
        self.setFreqB(1024)
        return self.lockStatus()
    
    
if __name__=="__main__":
    import Pyro.naming
    Pyro.config.PYRO_MULTITHREADED = 0
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('ValonServer')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    vs = ValonServer()
    daemon.connect(vs,"ValonServer")
    daemon.requestLoop()
