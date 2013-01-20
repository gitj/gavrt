"""
:mod:`dss28core.iBOBUtilServer`
-------------------------------

The IBOB Utility Server. 

Intended to be automatically run as stand alone module in background to provide Pyro interface to:

* JTAG programmers
* LabJacks that control ADC clock syntehsizers
* RS-232 ports for debug communication

Each bank of 4 iBOBs has a JTAG progammer and an RS-232 USB cable to program and communicate. A LabJack is used to control a simple multiplexing
circuit to share the programmer and RS-232 connection between the 4 iBOBs. In addition, each bank has an AD9517 synthesizer which provdes sampling
clocks for the ADCs. The syntehsizer is also controlled via the LabJack.

In practice, the methods used are:

* :meth:`~iBOBUtilServer.programIBOBs`
* :meth:`~iBOBUtilServer.setSynths`
* :meth:`~iBOBUtilServer.lockStatus`
"""

import time,os
import serial

import Pyro.naming
import Pyro.core

import myu3 as u3
import AD9517 as AD
import gavrtdb
from jtagprogram import prog_ipf,identify_cables

from loggers import corelog

class iBOBUtilServer(Pyro.core.ObjBase):
    def __init__(self,sql = True):
        """
        *sql* argument is largely a relic, no longer used without database
        """
        Pyro.core.ObjBase.__init__(self)
        Pyro.config.PYRO_MULTITHREADED = 0 # do not use multiple threads, because we want to ensure programming finishes
        if sql:
            self.sql = True
            corelog.debug("Connecting to GavrtDB")
            self.gdb = gavrtdb.GavrtDB(rw = True)
        else:
            self.sql = False
            self.gdb = None
            
        self.iBOBProcesses = {}
        self.iBOBServers = {}
        self.serialPorts = [None,None]
        self.lj0 = None
        self.lj1 = None
        self.ljs = [None,None]
        self.syns = [None,None]
        self.syn0 = None
        self.syn1 = None
        self.jtagcables = ['usb21','usb22']
        corelog.debug("Identifying JTAG programmers from list: %s" % str(self.jtagcables))
        try:
            self.identifyCables()
        except:
            corelog.exception("Could not identify JTAG programmers!")
        corelog.info("JTAG cables identified")
        corelog.debug("Opening LabJacks")
        try:
            self.openLabJacks()
        except:
            corelog.exception("Could not open LabJacks!")
            
        # Check if synthesizers are already initialized before clobbering them.
        if not self.synsInitialized():
            corelog.info("found synthesizers uninitialized, so initializing to 1024 MHz")
            self.initialize()
            self.setSynths(1024.0)
        corelog.info("Started")

    def ping(self):
        return True
    
    def identifyCables(self):
        """
        Sort out which JTAG cable is which
        """
        self.jtagcables = identify_cables()[0]
        
    def synsInitialized(self):
        """
        Check if synthesizers have been initialized yet
        """
        if self.synRegRead(0, 0x10) == 0x7d or self.synRegRead(1, 0x10) ==0x7d:
            return False
        else:
            return True
        
    def initialize(self):
        self.syn0.initialize()
        self.syn1.initialize()
    
    def openLabJacks(self):
        self.lj0 = u3.U3(localId=3)
        self.lj1 = u3.U3(localId=2)
        self.ljs = [self.lj0,self.lj1]
        self.syn0 = AD.AD9517(self.lj0)
        self.syn1 = AD.AD9517(self.lj1)
        self.syns = [self.syn0,self.syn1]
        
    def openSerial(self,ports=['/dev/ttyUSB2','/dev/ttyUSB3']):
        """
        Open serial communications with iBOBs. Requires list of serial ports which iBOBs are connected to
        """
        for n in range(len(self.serialPorts)):
            s = self.serialPorts[n]
            if s is not None:
                s.close()
            if ports[n]:
                s = serial.Serial(ports[n])
                self.serialPorts[n] = s
                s.setBaudrate(115200)
                s.setDTR(0)
                s.setRTS(1)
                s.setTimeout(0)
            else:
                print "Skipping port",n
                
    def closeSerial(self):
        for s in self.serialPorts:
            try:
                s.close()
            except:
                pass
            
    def synRegRead(self,syn,reg):
        """
        Read a register from a synthesizer. Low level debugging only
        """
        if syn ==0:
            s = self.syn0
        else:
            s = self.syn1
        return s.ReadReg(reg)
            
    
    def serialSend(self,msg,ibob,wait = 0.1):
        """
        Send a string to an ibob and get response
        
        Arguments:
        
            *msg* : string
                message to send
                
            *ibob* : int 0-7
                which ibOB to select
                
            *wait* : float (optional)
                how many seconds to wait for response
        """
        if ibob < 4:
            self.syn0.seliBOB(ibob)
            s = self.serialPorts[0]
        else:
            self.syn1.seliBOB(ibob-4)
            s = self.serialPorts[1]
        s.write(msg+'\n')
        time.sleep(wait)
        return s.read(s.inWaiting())
              
    def serialPing(self,msg):
        # this function looks like it should be removed...
        for s in self.serialPorts:
            s.write(msg+'\n')
        time.sleep(0.1)
        resp = []
        for s in self.serialPorts:
            resp.append(s.read(s.inWaiting()))
        return resp
    
    def lockStatus(self):
        """
        Get synthesizer lock status. Returns a boolean for each synthesizer
        """
        
        s0,s1 = self.syn0.isLocked(),self.syn1.isLocked()
        if not (s0 and s1):
            corelog.warning("ADC Synthesizers are not locked: Syn0 %s Syn1 %s" % (s0,s1))
        else:
            corelog.info("ADC Synthesizers are in lock")
        return s0,s1

    def setSynths(self,freq,force=False):
        """
        Set synthesizers to given frequency
        
        Arguments:
        
            *freq* : float
                Frequency in MHz
            
            *force* : bool (optional)
                if freq is same as current frequency of synthesizer, will not change the frequency unless *force* is True
        """
        if self.syn0.freq != freq or force:
            corelog.info("Setting syn0 to %f" % freq)
            s0 = self.setSynth(freq, 0)
        else:
            corelog.info("Syn0 was already at %f" % freq)
            s0 = None
        if self.syn1.freq != freq or force:
            corelog.info("Setting syn1 to %f" % freq)
            s1 = self.setSynth(freq, 1)
        else:
            corelog.info("Syn1 was already at %f" % freq)
            s1 = None
        status = self.lockStatus()
        if not (status[0] and status[1]):
            raise Exception("At least one synthesizer is out of lock! %s" % str((s0,s1,status)))
        return (s0,s1,status)
    def setSynth(self,freq,bank):
        if bank == 0:
            settings = self.syn0.setFreq(freq)
        else:
            settings = self.syn1.setFreq(freq)
        if settings is None:
            raise Exception("frequency is unreachable")
        fset = settings['output']
        
        if self.sql:
            vald = {}
            
            for ibob in range(4):
                ibn = ibob + 4*bank
                vald['iBOBADCClock%d' % ibn] = fset
            vald['UnixTime'] = time.time()
            try:
                self.gdb.updateValues(vald, 'spss_config')
            except:
                corelog.exception("Could not record synthesizer setting in Database!")
            
        return settings
    
    def selectIBOB(self,ibob):
        """
        Select a given ibob with multiplexer circuits
        """
        if ibob < 4:
            self.syn0.seliBOB(ibob)
            corelog.debug("bank 0, selected ibob %d" %ibob)
        else:
            ibobsel = ibob - 4
            self.syn1.seliBOB(ibobsel)
            corelog.debug("bank 1, selected ibob %d (sel: %d)" %(ibob,ibobsel))
        
    def programIBOBs(self,ibobs,ipfid=None,ipffile=None,force=False):
        """
        Program a list of iBOBs with a given configuration file.
        The configuration file can be explicitly provided or an index to the database of designs can be given
        
        Arguments:
        
            *ibobs* : list of ints
                Which ibobs to program
            *ipfid* : int
                Record ID of row in dss28_spec.ibob_designs that indicates the desired configuration
            *ipffile* : str (advanced use only)
                Instead of a design in the database, use this IPF explicitly
            *force* : bool
                Program the ibob even if we think it may already be programmed with same design.
        """
        if self.sql:
            if ipfid:
                try:
                    resd = self.gdb.getIPF(ipfid)
                    ipf = resd['IPF']
                    ipfdir = resd['Directory']
                    ipffile = os.path.join(ipfdir,ipf)
                except Exception, e:
                    corelog.exception("Could not resolve programming file from database")
                    raise e
                corelog.debug("Found ipfID: %d correspoing to %s" % (ipfid, ipffile))
            else:
                ipfid = 1 #we are connected to database, but programming an arbitrary file, so
                            # tell the database that the design is default
                            
            if force == False:
                droplist = []
                stat = self.gdb.getSPSSStatus()
                for ii in ibobs:
                    if stat["iBOBDesignID%d" % ii] == ipfid:
                        droplist.append(ii)
                        corelog.info("iBOB%d is already programmed with this ipf, skipping" % ii)
                for k in droplist:
                    ibobs.remove(k)
        if ipffile is None:
            print "no file selected!"
            return
        vald = {}
        results = []
        for ibob in ibobs:
            if ibob < 4:
                self.syn0.seliBOB(ibob)
                time.sleep(0.1)
                corelog.info("Starting to program ibob %d" % ibob)
                try:
                    r = prog_ipf(ipffile,cable=self.jtagcables[0],display=True)
                except Exception, e:
                    corelog.exception("Failure programming ibob %d with ipffile %s" % (ibob, ipffile))
                    raise e
                results.append(r)
                if r[0] == 0:
                    corelog.info("Programming ibob %d was successful" % ibob)
                    corelog.debug("Impact status for programming ibob %d was:\n\n %s \n\n" % (ibob,r[1]))
                else:
                    corelog.error("Programming ibob %d Failed! Impact returned: %d Status info:\n\n %s \n\n" % (ibob,r[0],r[1]))
            else:
                ibobsel = ibob - 4
                self.syn1.seliBOB(ibobsel)
                time.sleep(0.1)
                corelog.info("Starting to program ibob %d" % ibob)
                try:
                    r = prog_ipf(ipffile,cable=self.jtagcables[1],display=True)
                except Exception, e:
                    corelog.exception("Failure programming ibob %d with ipffile %s" % (ibob, ipffile))
                    raise e
                results.append(r)
                if r[0] == 0:
                    corelog.info("Programming ibob %d was successful.  Impact returned: %d Status info:\n\n %s \n\n" % (ibob,r[0],r[1]))
                else:
                    corelog.error("Programming ibob %d Failed! Impact returned: %d Status info:\n\n %s \n\n" % (ibob,r[0],r[1]))
            if results[-1][0] ==0:
                vald["iBOBDesignID%d" % ibob] = ipfid
            else:
                vald["iBOBDesignID%d" % ibob] = 1 # design 1 means unknown state
        
        corelog.debug("Updating status in db")
        vald['UnixTime'] = time.time()
        if self.sql:
            self.gdb.updateValues(vald, 'spss_config')
        return results
    
    
if __name__=="__main__":
    import Pyro.naming
    
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('UtilServer')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    daemon.connect(iBOBUtilServer(),"UtilServer")
    daemon.requestLoop()