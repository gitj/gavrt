"""
:mod:`AD9517`
-------------
Interface to control AD9517 Synthesizers via LabJack.

These synthesizers provide the ADC sampling clock in DSS-28 digital backend.

Also provides commands to select iBOBs connected to JTAG/RS232 Multiplexer board.
"""
from __future__ import with_statement
from __future__ import division
import numpy as np
import csv
from loggers import corelog

#"Addr(Hex)","Value(Bin)","Value(Hex)"
# this default configuration provides a known setup that works
default_config = [
(0x000, 0x18),
(0x001, 0x00),
(0x002, 0x50),
(0x003, 0x53),
(0x004, 0x00),
(0x010, 0x7C),
(0x011, 0x64),
(0x012, 0x00),
(0x013, 0x00),
(0x014, 0x40),
(0x015, 0x00),
(0x016, 0x06),
(0x017, 0x94),
(0x018, 0x07),
(0x019, 0x00),
(0x01A, 0x00),
(0x01B, 0x27),
(0x01C, 0x02),
(0x01D, 0x00),
(0x01E, 0x00),
(0x01F, 0x4F),
(0x0A0, 0x01),
(0x0A1, 0x00),
(0x0A2, 0x00),
(0x0A3, 0x01),
(0x0A4, 0x00),
(0x0A5, 0x00),
(0x0A6, 0x01),
(0x0A7, 0x00),
(0x0A8, 0x00),
(0x0A9, 0x01),
(0x0AA, 0x00),
(0x0AB, 0x00),
(0x0F0, 0x08),
(0x0F1, 0x08),
(0x0F4, 0x08),
(0x0F5, 0x08),
(0x140, 0x43),
(0x141, 0x43),
(0x142, 0x43),
(0x143, 0x43),
(0x190, 0x00),
(0x191, 0x80),
(0x192, 0x00),
(0x196, 0x11),
(0x197, 0x00),
(0x198, 0x00),
(0x199, 0x22),
(0x19A, 0x00),
(0x19B, 0x11),
(0x19C, 0x00),
(0x19D, 0x00),
(0x19E, 0x22),
(0x19F, 0x00),
(0x1A0, 0x11),
(0x1A1, 0x00),
(0x1A2, 0x00),
(0x1A3, 0x00),
(0x1E0, 0x00),
(0x1E1, 0x02),
(0x230, 0x00),
(0x231, 0x00),
(0x232, 0x00),
]


class AD9517():
    def __init__(self,lj):
        self.lj = lj
        self.freq = None
        self.CS = 13
        self.SCK = 14
        self.SDIO = 15
        self.SDO = 12
#        self.lj.BitDirWrite(self.CS,1)
#        self.lj.BitDirWrite(self.SCK,1)
#        self.lj.BitStateWrite(self.CS,1)
#        self.lj.BitStateWrite(self.SCK,1)
        if self.lj is not None:
            self.lj.configIO(FIOAnalog=0,EIOAnalog=0)
            self.lj.setFIOState(self.CS,1)
            self.lj.setFIOState(self.SCK,1)
        
    def seliBOB(self,n):
        """
        Select an iBOB for RS232 comms or JTAG programming
        """
        n = int(n)
        A0 = 16
        A1 = 17
        A2 = 18
#        self.lj.BitDirWrite(A0,1)
#        self.lj.BitDirWrite(A1,1)
#        self.lj.BitDirWrite(A2,1)
        if n & 0x01:
#            self.lj.BitStateWrite(A0,1)
            self.lj.setFIOState(A0,1)
        else:
#            self.lj.BitStateWrite(A0,0)
            self.lj.setFIOState(A0,0)
        if n & 0x02:
#            self.lj.BitStateWrite(A1,1)
            self.lj.setFIOState(A1,1)
        else:
#            self.lj.BitStateWrite(A1,0)
            self.lj.setFIOState(A1,0)
        if n & 0x04:
#            self.lj.BitStateWrite(A2,1)
            self.lj.setFIOState(A2,1)
        else:
#            self.lj.BitStateWrite(A2,0)
            self.lj.setFIOState(A2,0)
    def isLocked(self):
        """
        Check lock status
        """
        return (self.ReadReg(0x1F) & 0x43) == 0x43
    def setFreq(self,f):
        """
        Set output frequency in MHz
        """
        corelog.debug("Setting frequency to %f" %f)
        settings = self.calcPLL(f)
        self.setPLL(settings)
        self.freq = f
        return settings
    def setOutputLevel(self,output,level):
        """
        Set output RF level for given output.
        
        Arguments:
        
            output : integer 0-3
            level : integer 0-3  
        """
        assert (output >= 0 and output <=3)
        assert (level >= 0 and level <=3)
        regs = [0xF0, 0xF1, 0xF4, 0xF5]
        self.WriteReg(regs[output], level<<2)
    def calcPLL(self,f):
        """
        Calculate the PLL parameters
        """
        if f > 1125:
            corelog.error("Attempting to set frequency higher than 1125 MHz. Value was: %f MHz" % f)
            return None
        finaldiv = 1
        while f*finaldiv < 291:
            finaldiv *= 2
        if finaldiv > 32:
            corelog.error("IBOB ADC clock synthesizer cannot reach that frequency. would require a final divider of %d > 32" % finaldiv)
            return None
        forig = f
        f = f*finaldiv
        if f >=875 and f <=1125:
            VCOdiv = 2
        elif f >=583 and f <= 750:
            VCOdiv = 3
        elif f >= 438 and f <= 562:
            if finaldiv < 32:
                f*=2
                finaldiv *= 2
                VCOdiv = 2
            else:
                VCOdiv = 4
        elif f >= 350 and f <= 450:
            VCOdiv = 5
        elif f >= 291 and f <= 375:
            if finaldiv < 32:
                finaldiv *= 2
                f*=2
                VCOdiv = 3
            else:
                VCOdiv = 6
        else:
            corelog.error("IBOB ADC clock synthesizer cannot reach that frequency. forig: %f f: %f finaldiv: %d" % (forig, f, finaldiv))
            return None
        (b,a) = divmod(f*VCOdiv,8)

        output = (b*8+a)/VCOdiv/finaldiv
        corelog.info("Found PLL settings: forig: %f, f: %f, finaldiv: %d, VCOdiv: %d, b: %d, a: %d, VCO: %f expected output: %f", 
                     (forig,f,finaldiv,VCOdiv, b, a, f*VCOdiv, output))
        return dict(VCOdiv=VCOdiv,b=b,a=a,finaldiv=finaldiv,VCO=f,output=output)
        
    def setPLL(self,d):
        if d is None:
            return
        corelog.debug("Setting prescaler = 8/9")
        self.WriteReg(0x16,0x04)
        corelog.debug("Setting r = 100 (100MHz ref)")
        self.WriteReg(0x11,100)
        corelog.debug("Setting a: %d" % d['a'])
        self.WriteReg(0x13,d['a'])
        (h,l) = divmod(d['b'],256)
        corelog.debug("Setting b: %d=%d*256+%d" % (d['b'],h,l))
        self.WriteReg(0x14,l)
        self.WriteReg(0x15,h)
        VCOdivVal = d['VCOdiv']-2
        if VCOdivVal < 0 or VCOdivVal > 4:
            corelog.error("Got bad VCOdivider: %d Original valud %d. Proceeding using 2" % (VCOdivVal,d['VCOdiv']))
            VCOdivVal = 0
        corelog.debug("Setting VCOdiv: %d, reg: %d" % (d['VCOdiv'],VCOdivVal))
        self.WriteReg(0x1E0,VCOdivVal)
        finaldiv = d['finaldiv']
        if finaldiv == 1:
            corelog.debug("Bypassing final divider")
            self.WriteReg(0x197,0x80)
            self.WriteReg(0x191,0x80)
        else:
            cycles = finaldiv/2 - 1
            corelog.debug("cycles= %d cyclereg: 0x%02x" % (cycles,cycles*16+cycles))
            self.WriteReg(0x197,0x00)
            self.WriteReg(0x191,0x00)
            self.WriteReg(0x196,cycles*16+cycles)
            self.WriteReg(0x190,cycles*16+cycles)
        self.WriteReg(0x232,1)
        corelog.debug("Calibrating VCO")
        self.WriteReg(0x18,0x06)
        self.WriteReg(0x232,1)
        self.WriteReg(0x18,0x07)
        self.WriteReg(0x232,1)
    
    def setDivider1(self,div):
        if div == 1:
            print "bypassing divider"
            self.WriteReg(0x197, 0x80)
        else:
            cycles = div/2 - 1
            print "cycles=",cycles,"cyclereg: %02x"%(cycles*16+cycles)
            self.WriteReg(0x197, 0x00)
            self.WriteReg(0x196, cycles)
    def EnableAll(self):
        self.WriteReg(0xF0, 0x08)
        self.WriteReg(0xF1, 0x08)
        self.WriteReg(0xF4, 0x08)
        self.WriteReg(0xF5, 0x08)
        self.WriteReg(0x232, 1)
    def Start(self):
#        self.lj.BitStateWrite(self.CS,0)
        self.lj.setFIOState(self.CS,0)
    def Stop(self):
#        self.lj.BitStateWrite(self.CS,1)
        self.lj.setFIOState(self.CS,1)
    def WriteBytes(self,bs):
        for b in bs:
            self.WriteByte(b)
            
        
    def WriteByte(self,b):
        bit = 0x80
#        self.lj.BitStateWrite(self.SCK,0)
        self.lj.setFIOState(self.SCK,0)
#        self.lj.BitDirWrite(self.SDIO,1)
        self.lj.setFIODir(self.SDIO,1)
        while bit:
#            self.lj.BitStateWrite(self.SCK,0)
            self.lj.setFIOState(self.SCK,0)
            if (b & bit):
#                self.lj.BitStateWrite(self.SDIO,1)
                self.lj.setFIOState(self.SDIO,1)
            else:
#                self.lj.BitStateWrite(self.SDIO,0)
                self.lj.setFIOState(self.SDIO,0)
#            self.lj.BitStateWrite(self.SCK,1)
            self.lj.setFIOState(self.SCK,1)
            bit >>= 1
    def ReadByte(self):
        b = 0
#        self.lj.BitStateWrite(self.SCK,0)
#        self.lj.BitDirWrite(self.SDIO,0)
        self.lj.setFIOState(self.SCK,0)
        self.lj.setFIODir(self.SDIO,0)
        for k in range(8):
            b <<= 1
#            self.lj.BitStateWrite(self.SCK,0)
            self.lj.setFIOState(self.SCK,0)
            
#            if self.lj.BitStateRead(self.SDIO)[0]:
            print self.lj.getFIOState(self.SDIO),
            if self.lj.getFIOState(self.SDIO):
                b += 1
#            self.lj.BitStateWrite(self.SCK,1)
            self.lj.setFIOState(self.SCK,1)
        return b
    
    def ReadReg(self,reg):
        self.Start()
        self.WriteBytes([((reg & 0x0F00)>>8) | 0x80,(reg & 0xFF)])
        b = self.ReadByte()
        self.Stop()
        print hex(b)
        return b
    
    def WriteReg(self,reg,val):
        self.Start()
        reg = int(reg)
        val = int(val)
        self.WriteBytes([(reg & 0x0F00)>>8,(reg & 0xFF),val])
        self.Stop()
    
    def initialize(self):
        self.fromList()
        
    def fromList(self,config=default_config):
        """
        Initialize synthesizer from list of register values
        """
        for r in config:
            self.WriteReg(r[0],r[1])
        self.WriteReg(0x232,1)
        corelog.info("Initialized ADC Synthesizer")            
    def fromFile(self,fname):
        with open(fname,'r') as f1:
            rd = csv.reader(f1)
            lst = []
            
            for k in rd:
                lst.append(k)
            data = []    
            for k in lst[4:67]:
                data.append((int(k[0],16),int(k[2],16)))
            
            print "Writing to Registers..."    
            for r in data:
                self.WriteReg(r[0],r[1])
            print "Latching Registers"
            self.WriteReg(0x232,1)
            
def readFile(fname):
    with open(fname,'r') as f1:
        rd = csv.reader(f1)
        lst = []
        
        for k in rd:
            lst.append(k)
        data = []    
        for k in lst[4:67]:
            data.append((int(k[0],16),int(k[2],16)))
    return data
        

