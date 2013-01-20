"""
:mod:`rss`
==========

This module provides the :class:`~rss.RSS` class for controlling the RSS but also
provides many constants to avoid ambiguity of paramters and to allow tab completion
in observing scripts.

When using the RSS methods, paramters should be specified as follows::

    import rss
    obs.rss.HF.set(noiseX=rss.ON, basis=rss.CIRC)
    
rather than as::

    obs.rss.HF.set(noiseX = 'ON', basis='CIRC')
    
Examples::

    obs.rss.set(matrix={0:'1AL', 4:5})    #Connect iBOB0 to RX1,polA,LSB and iBOB4 to RX2,polA,LSB
    
    obs.rss.HF.set(noiseX=rss.OFF, noiseY=rss.OFF) # Turn HFF noise diodes off
    
    obs.rss.RX1.set(f0=5400)    # Set the RX1 synthesizer so that the f0 frequency is 5400 MHz
    obs.rss.RX1.set(synth=6850) # This is equivalent to the above statement since f0=5400 corresponds to synth=6850
    
    obs.rss.RX1.A.set(atten=20.0) # set the attenuation to 20dB
    
    # Select HFF, wide bandwidth (2000 MHz) IF filter, no baseband filter (0-1000 MHz), and I/Q mode
    obs.rss.RX3.B.set(ifbw=rss.WIDE, feed=rss.HF, bbfilt=rss.THRU, mode=rss.IQ) 
    
    # Select LFF, narrow bandwidth (400 MHz) IF filter, bandpass baseband filter (270-370 MHz) and USB/LSB mode
    obs.rss.RX2.A.set(ifbw=rss.NARROW, feed=rss.LF, mode=rss.UL, bbfilt=rss.BPF)
                    
"""
import numpy as np
import time
import gavrtdb
from gavrt_constants import ibob_fiber_map

try:
    import Pyro.naming
except:
    pass
HF = 'HF'
HFF = 'HF'
_HFequiv = [HF,'HFF']
LF = 'LF'
LFF = 'LFF'
_LFequiv = [LF,'LFF']
NARROW = '400'
WIDE = '2000'
_NARROWequiv = [NARROW,400,'NARROW']
_WIDEequiv = [WIDE,2000,'WIDE']
IQ = 'IQ'
UL = 'UL'
LPF = 'LPF'
BPF = 'BPF'
HPF = 'HPF'
THRU = 'THRU'
EXT = 'EXT'
TERM = 'TERM'
LIN = 'LIN'
CIRC = 'CIRC'
NORM = 'NORM'
REV = 'REV'
OFF = 'OFF'
ON = 'ON'
NAR = 'NAR'
COMB = 'COMB'

filter_bandwidths = {THRU: (0,1050),
                     LPF: (0,550),
                     BPF: (270,370),
                     HPF: (500,1050),
                     TERM: (-1,-1),
                     UL: (50,2000),
                     IQ: (1,2000),
                     NARROW: (30,500),
                     WIDE: (0,1050)}

def bwAnd(x,y):
    """ Resulting bandwidth if two filters are cascaded """
    low = np.max((x[0],y[0]))
    high = np.min((x[1],y[1]))
    return (low,high)

chan_names = ['%d%s%s' % (n+1,p,sb) for n in range(4) for p in ['A','B'] for sb in ['U','L']] #1AU, 1AL, 1BU, 1BL, 2AU, 2AL,...
chan_name_number_map = {}
for n,name in enumerate(chan_names):
    chan_name_number_map[name] = n
    
def channelInfo(channum):
    chn = chan_names[channum]
    rx = int(chn[0])
    pol = chn[1]
    if chn[2] == 'U':
        sb = 1
    else:
        sb = -1
    return rx,pol,sb

def matrixfromibobs(d):
    """
    Convert dictionary of ibob:channel pairs to switch matrix configuration
    
    *d*: keys are ibob number (0-7)
    values are channel names 1AU-4BL as strings or numbers for channels (0-15)
    """
    mat = {}
    for ibob,chan in d.items():
        fiber = ibob_fiber_map[ibob]
        if isinstance(chan,str):
            chnum = chan_name_number_map[chan]
        else:
            chnum = chan
        mat[fiber] = chnum
    return mat
    



class Feed(object):
    """
    Representation of the attributes of either the HFF or LFF.
    
    This class is intended only to be instantiated as part of the :class:`rss.RSS` master class
    """
    def __init__(self,parent,feed):

        self.parent = parent
        self.noiseX = OFF
        self.noiseY = OFF
        self.comb = False
        self.basis = LIN
        self.transfer = NORM
        self.feed = feed
    def restore(self):
        self.set(noiseX=self.noiseX, noiseY=self.noiseY, basis=self.basis, transfer=self.transfer,submit=False)
    def set(self,noiseX=None,noiseY=None,basis=None,transfer=None, submit=True):
        """
        *noiseX* - State of pol. X noise diode. Can be rss.ON, rss.OFF, rss.COMB (phase cal), rss.NAR (for noise added radiometer mode)
        
        *noiseY* - State of pol. Y noise diode. Can be rss.ON, rss.OFF, or rss.NAR (for noise added radiometer mode)
        
        *basis* - Polarization basis. Can be rss.LIN for linear or rss.CIRC for circular
        
        *transfer* - Polarization transfer switch state. Can be rss.NORM for normal or rss.REV for reversed
        """
        if noiseX:
            noiseX = noiseX.upper()
        if noiseY:
            noiseY = noiseY.upper()
        if basis:
            basis = basis.upper()
        if transfer:
            transfer = transfer.upper()
        if noiseX is not None:
            if noiseX not in [OFF,ON,NAR,COMB]:
                raise Exception("Bad NoiseX Value",noiseX)
            self.noiseX = noiseX
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
            self.parent.send_cmd("ND A %s" % self.noiseX) # 2011.08.06 - appears John Leflang has renamed pol X as pol A, pol Y as pol B
            
        if noiseY is not None:
            if noiseY not in [OFF,ON,NAR,COMB]:
                raise Exception("Bad NoiseY Value",noiseY)
            self.noiseY = noiseY
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
            self.parent.send_cmd("ND B %s" % self.noiseY)
        
        sendpol = False
        
        if basis is not None:
            if basis not in [LIN,CIRC]:
                raise Exception("Bad Basis Value",basis)
            self.basis = basis            
            sendpol = True
        
        if transfer is not None:
            if transfer not in [NORM,REV]:
                raise Exception("Bad Transfer Value",transfer)
            self.transfer = transfer
            sendpol = True
        
        if sendpol:
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
        
            self.parent.send_cmd("POL %s %s %s" % (self.feed,self.basis,self.transfer))
        if submit:
            self.parent.addToDB()
            
    def updateRecord(self,rec):
        rec["%sPolXNoise" % (self.feed)] = self.noiseX
        rec["%sPolYNoise" % (self.feed)] = self.noiseY
        rec["%sCombGen" % (self.feed)] = self.comb
        rec["%sPolBasis" % (self.feed)] = self.basis
        rec["%sPolTransfer" % (self.feed)] = self.transfer
        
class Polarization(object):
    """
    Representation of the attributes of a single polarization channel of a receiver plate.
    
    This class is intended only to be instantiated as part of the :class:`rss.RSS` master class
    """
    def __init__(self,parent,rx,pol):
        self.rx = rx
        self.pol = pol
        self.parent = parent
        self.feed = HF
        self.ifbw = NARROW
        self.mode = UL
        self.atten = 31.5
        self.bbfilt = BPF
    def restore(self):
        self.set(feed=self.feed, ifbw=self.ifbw, mode=self.mode, atten=self.atten, bbfilt=self.bbfilt,submit=False)
    def set(self,feed=None,ifbw=None,mode=None,atten=None,bbfilt=None,submit=True):
        """
        *feed* - Which feed this channel is connected to. Can be rss.HF or rss.LF
        
        *ifbw* - IF filter bandwidth. Can be rss.NARROW or rss.WIDE
        
        *mode* - Sideband mode. Can be rss.UL for USB/LSB or rss.IQ for I/Q mode
        
        *atten* - Digital attenuator setting. 0-31.5 dB in 0.5dB steps
        
        *bbfilt* - Baseband filter setting. Options are:
        
        * rss.THRU - no filter, full bandwidth. Bandwidth will be 0-400 MHz for ifbw=rss.NARROW or 0-1000 MHz for ifbw=rss.WIDE
        * rss.LPF - 0-500 MHz low pass filter. 
        * rss.HPF - 500-1000 MHz high pass filter.
        * rss.BPF - 270-370 MHz band pass filter
        * rss.TERM - Terminated. Useful for measuring power level offsets
        * rss.EXT - External filter (not currently implemented, so essentially same as rss.TERM)
        """
        
        if feed:
            feed = feed.upper()
        if mode:
            mode = mode.upper()
        if bbfilt:
            bbfilt = bbfilt.upper()
        if feed is not None:
            if feed not in _HFequiv+_LFequiv:
                raise Exception("Bad feed Value",feed)
            if feed in _HFequiv:
                feed = HF
            else:
                feed = LF
            self.feed = feed
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
                
            print "warning: setting feed currently affects all receiver channels!"
            # This is the real command we want to send, but doesn't exist yet
            #self.parent.send_cmd("FEED %d%s %s" % (self.rx,self.pol,self.feed))
            #so insstead we send this:
            
            self.parent.send_cmd("FEED %s" % self.feed)
            
            
        if ifbw is not None:
            if ifbw not in _NARROWequiv+_WIDEequiv:
                raise Exception("Bad ifbw Value",ifbw)
            if ifbw in _NARROWequiv:
                ifbw = NARROW
            else:
                ifbw = WIDE
            self.ifbw = ifbw
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
            self.parent.send_cmd("IF %d%s %s" % (self.rx,self.pol,self.ifbw))
            
        
        if mode is not None:
            if mode not in [UL,IQ]:
                raise Exception("Bad mode Value",mode)
            self.mode = mode
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
            self.parent.send_cmd("MODE %d%s %s" % (self.rx,self.pol,self.mode))
        
        if bbfilt is not None:
            if bbfilt not in [LPF, BPF, HPF, THRU, EXT, TERM]:
                raise Exception("Bad bbfilt Value",bbfilt)
            self.bbfilt = bbfilt
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
            self.parent.send_cmd("BBF %d%s %s" % (self.rx,self.pol,self.bbfilt))
            
        if atten is not None:
            if atten not in np.arange(0,32,0.5):
                raise Exception("Bad atten Value",atten)
            self.atten = atten
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
            self.parent.send_cmd("ATTEN %d%s %.1f" % (self.rx,self.pol,self.atten))
            
        if submit:
            self.parent.addToDB()
    
    def updateRecord(self,rec):
        rec["RX%d%s_Feed" % (self.rx,self.pol)] = self.feed
        rec["RX%d%s_IFFilter" % (self.rx,self.pol)] = self.ifbw
        rec["RX%d%s_Mode" % (self.rx,self.pol)] = self.mode
        rec["RX%d%s_BBFilter" % (self.rx,self.pol)] = self.bbfilt
        rec["RX%d%s_Atten" % (self.rx,self.pol)] = self.atten
            
class ReceiverPlate(object):
    """
    Representation of the attributes of a receiver plate.
    
    This class is intended only to be instantiated as part of the :class:`rss.RSS` master class
    """

    def __init__(self,parent,rx):
        self.rx = rx
        self.parent = parent
        self.synth = 8500.0
        self.A = Polarization(parent,self.rx,'A')
        self.B = Polarization(parent,self.rx,'B')
        
    def restore(self):
        self.A.restore()
        self.B.restore()
        self.set(synth=self.synth,submit=False) # must set LO last because of bug in RCT #146
    def set(self,synth=None,f0=None, submit = True):
        """
        *f0* - Desired center frequency (center of USB/LSB) in MHz. Note some choices of f0 will be rounded to nearest possible frequency.
        
        *synth* - Desired synthesizer setting in MHz. Valid range 5500-9999.9 in 0.1 MHz steps
        
        If both *f0* and *synth* are provided, f0 will override.
        """
        if f0 is not None:
            synth = (22000+f0)/4.0
        if synth is not None:
            if synth > 9999.9:
                raise Exception("Bad synth",synth)
            if synth < 5500.0:
                raise Exception("Bad synth",synth)
            self.synth = synth
            if not self.parent._dirty:
                self.parent._dirty = True
                self.parent._starttime = time.time()
            self.parent.send_cmd("SETLO %d %.1f" % (self.rx,self.synth))
            
            if submit:
                self.parent.addToDB()
        
    def updateRecord(self,rec):
        rec["RX%d_Synth" % self.rx] = self.synth
        self.A.updateRecord(rec)
        self.B.updateRecord(rec)

class RSSState(object):
    """
    Class to hold current RSS state
    
    This class is intended only to be instantiated as part of the :class:`rss.RSS` master class
    """
    def __init__(self,parent):
        self.parent = parent
        self.RX1 = ReceiverPlate(parent,1)
        self.RX2 = ReceiverPlate(parent,2)
        self.RX3 = ReceiverPlate(parent,3)
        self.RX4 = ReceiverPlate(parent,4)
        self._matrix = np.zeros((8,),dtype='int')
        self.HF = Feed(parent,HF)
        self.LF = Feed(parent,LF)
    
        self.RXs = [self.RX1,self.RX2,self.RX3,self.RX4]
        self.PolA = [self.RX1.A,self.RX2.A,self.RX3.A,self.RX4.A]
        self.PolB = [self.RX1.B,self.RX2.B,self.RX3.B,self.RX4.B]
        self.Pols = [self.RX1.A,self.RX1.B,
                     self.RX2.A,self.RX2.B,
                     self.RX3.A,self.RX3.B,
                     self.RX4.A,self.RX4.B]
    def set_syns(self,syns=6750*np.ones((4,))):
        for n,rx in enumerate(self.RXs):
            rx.set(synth=syns[n],submit=False)
        self.parent.addToDB()
        
    def set_f0s(self,f0s = 4500*np.ones((4,))):
        for n,rx in enumerate(self.RXs):
            rx.set(f0=f0s[n],submit=False)
        self.parent.addToDB()
        
    def set_polA(self,**kwargs):
        kwargs['submit'] = False
        for p in self.PolA:
            p.set(**kwargs)
        self.parent.addToDB()
    def set_polB(self,**kwargs):
        kwargs['submit'] = False
        for p in self.PolB:
            p.set(**kwargs)
        self.parent.addToDB()
    def restore(self):
        self.RX1.restore()
        self.RX2.restore()
        self.RX3.restore()
        self.RX4.restore()
        self.HF.restore()
        self.LF.restore()
        matx = {}
        for fib,chan in enumerate(self._matrix):
            matx[fib] = chan
        
        self.set(matrix= matx, fullupdate=True)
        
    
        
    def set(self,matrix=None,fullupdate=False,submit=True):
        """
        Set the matrix switch using a dictionary to map ibobs to fibers
        
        *matrix* - dictionary representing mapping. keys are iBOBs, values are receiver channels.
        Receiver channels can be represented as a number 0-15 or as 1AU, 3BL, etc.
        
        *fullupdate* - If False, only sends commands to change fibers that are different from last state.
        """
        if matrix is not None:
            matrix = matrixfromibobs(matrix)
            tempmat = self._matrix.copy()
            for fib,chan in matrix.items():
                try:
                    if chan < 0 or chan > 15:
                        raise Exception("Channel out of range 0-15 got: %d" %chan)
                    if fib < 0 or fib > 7:
                        raise Exception("Fiber out of range 0-7 got: %d" % fib)
                    tempmat[fib] = chan
                    if fullupdate or (self._matrix[fib] != tempmat[fib]):
                        if not self.parent._dirty:
                            self.parent._dirty = True
                            self.parent._starttime = time.time()
                        self.parent.send_cmd("MATRIX %d %d" % (chan+1,fib+1))
                        
                except:
                    raise Exception("Bad matrix channel was:",chan,"fiber was",fib)
            self._matrix = tempmat
            
            if submit:
                self.parent.addToDB()

class DebugInterface():
    def __init__(self):
        pass
    def connect(self):
        pass
    def send_cmd(self,cmd):
        print "Debug RSS Interface command",cmd



class RSS():
    """
    The RSS class attributes reflect the heirarchical structure of the receiver as follows:
    
    * :class:`~rss.RSS` - The whole receiver. 
    
        * Use :meth:`~rss.RSSState.set` to configure: *matrix* 
    
        * *HF* - :class:`~rss.Feed` - The High Frequency Feed
        
            * Use :meth:`~rss.Feed.set` to configure: *noiseX*, *noiseY*, *comb*, *basis*, *transfer*
            
        * *LF* - :class:`~rss.Feed` - The Low Frequency Feed
        
            * Use :meth:`~rss.Feed.set` to configure: *noiseX*, *noiseY*, *comb*, *basis*, *transfer*
            
        * *RX1* - :class:`~rss.ReceiverPlate` - Receiver Plate 1
        
            * Use :meth:`~rss.ReceiverPlate.set` to configure: *synth*
            
            * *A* - :class:`rss.Polarization` - Pol A side of RX1
                
                * Use :meth:`~rss.Polarization.set` to configure: *ifbw*, *bbfilt*, *mode*, *atten*, *feed*
                
            * *B* - :class:`rss.Polarization` - Pol B side of RX1
            
                * Use :meth:`~rss.Polarization.set` to configure: *ifbw*, *bbfilt*, *mode*, *atten*, *feed*
            
        * *RX2* - :class:`~rss.ReceiverPlate` - Receiver Plate 2
        
            * ...
            
        * *RX3* - :class:`~rss.ReceiverPlate` - Receiver Plate 3
        * *RX4* - :class:`~rss.ReceiverPlate` - Receiver Plate 4
        
    Each attribute can be set using the *set* function of the corresponding class.
    
    .. automethod:: RSSState.set
    
    """
    def __init__(self,debug = False):
        self._debug = debug
        
        self.state = RSSState(self)
        self.RX1 = self.state.RX1
        self.RX2 = self.state.RX2
        self.RX3 = self.state.RX3
        self.RX4 = self.state.RX4
        self.HF = self.state.HF
        self.LF = self.state.LF
        self.set = self.state.set
        self.restore = self.state.restore
        self.set_syns = self.state.set_syns
        self.set_f0s = self.state.set_f0s
        self.set_polA = self.state.set_polA
        self.set_polB = self.state.set_polB
        
        self.chanbyname = {}
        self.rxbyname = {}
        for rx in range(1,5):
            for pol in ['A','B']:
                chan = getattr(getattr(self,'RX%d'%rx),pol)
                self.chanbyname['%d%s' % (rx,pol)] = chan
                for sb in ['U','L']:
                    self.chanbyname['%d%s%s' % (rx,pol,sb)] = chan 
                    self.rxbyname['%d%s%s' % (rx,pol,sb)] = getattr(self,'RX%d'%rx)
        
        
        self._dirty = False
        self._starttime = 0
        self._db = None
        
        if debug:
            self.interface = DebugInterface()
        else:
            self.ns = Pyro.naming.NameServerLocator().getNS()
            self.interface = self.ns.resolve('RSSInterface').getProxy()
            self.connectToDatabase()
            
            
    def connectToDatabase(self):
        self._db = gavrtdb.GavrtDB(True)
    
    def iBOBToChan(self,ibob):
        fiber = ibob_fiber_map[ibob]
        name = chan_names[self.state._matrix[fiber]]
        return self.chanbyname[name]
    
    def iBOBToChanRX(self,ibob):
        fiber = ibob_fiber_map[ibob]
        name = chan_names[self.state._matrix[fiber]]
        return (self.chanbyname[name],self.rxbyname[name])

    def send_cmd(self,cmd):
        print "cmd:",cmd
        print "response:", self.interface.send_cmd(cmd)
        
    def addToDB(self):
        if self._dirty:
            rec = {}
            for fiber in range(8):
                rec["Fiber%d" % fiber] = self.state._matrix[fiber]
            rec['StartTime'] = self._starttime
            rec['ReadyTime'] = time.time()
            self.RX1.updateRecord(rec)
            self.RX2.updateRecord(rec)
            self.RX3.updateRecord(rec)
            self.RX4.updateRecord(rec)
            self.HF.updateRecord(rec)
            self.LF.updateRecord(rec)
            if self._db:
                self._db.insertRecord('rss_config', rec)
            else:
                if self._debug:
                    print "would have inserted record:",rec
            self._dirty = False
        
    def updateFromDb(self):
        state = self._db.getLastRecord('rss_config')
        self.updateFromDict(state)
        
    def updateFromDict(self,state):
        for rxnum in range(1,5):
            rx = getattr(self,'RX%d'%rxnum)
            rx.synth = float(state['RX%d_Synth'%rxnum])
            for pol in ['A','B']:
                p = getattr(rx,pol)
                p.feed = state['RX%d%s_Feed'%(rxnum,pol)]
                p.ifbw = state['RX%d%s_IFFilter'%(rxnum,pol)]
                p.mode = state['RX%d%s_Mode'%(rxnum,pol)]
                p.bbfilt = state['RX%d%s_BBFilter'%(rxnum,pol)]
                p.atten = float(state['RX%d%s_Atten'%(rxnum,pol)])
        for feed in ['HF','LF']:
            fd = getattr(self,feed)
            fd.noiseX = state['%sPolXNoise'%feed]
            fd.noiseY = state['%sPolYNoise'%feed]
            fd.comb = [False,True][int(state['%sCombGen'%feed])]
            fd.basis = state['%sPolBasis'%feed]
            fd.transfer = state['%sPolTransfer'%feed]
        for fiber in range(8):
            self.state._matrix[fiber] = state['Fiber%d'%fiber]