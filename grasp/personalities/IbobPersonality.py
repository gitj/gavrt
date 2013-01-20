"""
iBOB Personalities
------------------

These are personalities for ibobs
"""
import time
import numpy as np
import gavrtdb

class IbobPersonality(object):
    """
    abstract base class from which actual personalities are inherited from
    
    If the *parent* is not None, it is assumed to be a Pyro proxy to a :class:`~dss28core.IbobServer.IbobServer`
    which implements methods such as regread, regwrite, sendget, etc.
    """
    def __init__(self,parent = None,adcClock=1024.0):
        
        self._parent = parent
        self._firstts = 0       # check, do we use this?
        self._lastacc = 0
        self._fullacc = 0
        self._t_int = 40e-3
        self._mode = 0
        
        self._measTypesDict = {}
        self._measTypes = self._measTypesDict.keys()
        self._infoTable = {}
        self._controlRegisters = {}
                                        
        if parent is not None:
            self._gdb = gavrtdb.GavrtDB(True)
            self._parent._setTimeout(10)
            self._ibobID = self._parent.get_id()
            
            self._sendget = parent.sendget_command
            self._write_register = parent.write_register
            self._regwrite = parent.write_register
            self.regread = parent.read_register
            self.sendget = parent.sendget_command
            self._write_info = parent.write_spec_info
    
    def readFromIbob(self):
        try:
            for reg in self._controlRegisters:
                self._controlRegisters[reg] = self.regread(reg)
        except Exception,e:
            print 'read_from_iBOB:',e
    def storeConfig(self,writedb=True):
        """
        Store current iBOB configuration in H5 file if available and optionally in database
        
        Intended to be called by StartRecord
        """
        idict = {}
        for key,value in self._controlRegisters.items():
            idict[key.replace('/','_')] = value 
        self._parent.write_spec_info(idict)
        if writedb:
            self._gdb.insertRecord('ibob_config', dict(UnixTime=time.time(), StatusDict=repr(self._controlRegisters), iBOB=self._ibobID))
    def regwrite(self,reg,val,writedb=True):
        try:
            if self._controlRegisters.has_key(reg):
                self._controlRegisters[reg] = val
                idict = {}
                for key,value in self._controlRegisters.items():
                    idict[key.replace('/','_')] = value 
                self._parent.write_spec_info(idict)
        except Exception, e:
            print reg,val,e
        if writedb:
            self._gdb.insertRecord('ibob_config', dict(UnixTime=time.time(), StatusDict=repr(self._controlRegisters), iBOB=self._ibobID))

        self._parent.write_register(reg,val)
    regWrite = regwrite
    def adcReset(self,interleave=False):
        """
        Reset the ADC
        
        *interleave* is False for designs that use the I and Q inputs, and True for designs that use just the I input.
        """
        if interleave:
            mode = 1
        else:
            mode = 0
        res = ("adcreset %d 0" % mode)
        return self._sendget(res)
        
    def startUdp(self,ip='192.168.0.2',mode=24,portOffset=0):
        if self._parent is None:
            raise Exception("No connection to iBOB available")
        ibob = self._ibobID
        octets = [int(x) for x in ip.split('.')]
        self.endUdp()
        if len(octets) != 4:
            raise Exception("bad IP:",ip)
        cmd = "startudp %d %d %d %d %d %d" % (octets[0],octets[1],octets[2],octets[3],59000+16+ibob+portOffset,mode)
        self._mode = mode
        return self._sendget(cmd)
    
    def endUdp(self):
        self._sendget('endudp')
    
    
    def requestAdcSnapshot(self):
        """
        Request an ADC snapshot
        args: none
        """
        return self._regwrite('snap/ctrl',7)
    
    def _reconstructMeasurement(self,m):    #this function will be called by the parser after the packets have been
                                            #reconstructed in order to convert the raw brams to meaningful data.
        raise("NotImplemented")
    
    def _bbfrq(self):
        return np.array([0])

    
class DummyPersonality(IbobPersonality):
    """
    Dummy Personality for testing new FPGA designs
    """
    pass
