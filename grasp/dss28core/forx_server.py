"""
:mod:`dss28core.forx_server`
----------------------------

This module is intended to run as a standalone program that reads monitor data from the Single Mode Fiber Receivers in the SPSS.
The monitor data is logged in the GavrtDB.

The fiber receiver monitor points (FORXMon*) should typically be between 2-4.5 V.

The 100 MHz reference transmitter monitor point (FOTXMon) should typically read ~ -1.4 V

The SPSS 1 Rack temperature at the LabJack data recorder is also recorded.

This should be started automatically, simply running forx_server

"""

import numpy as np
import sys, time
import myu3 as u3

import grasp.gavrtdb as gavrtdb
import time

from loggers import corelog

channelmap = {0:'Minus12V',
            1:'Plus12V',
            2:'FOTXMon',
            8:'FORXMon6',
            9:'FORXMon4',
            10:'FORXMon2',
            11:'FORXMon0',
            12:'FORXMon7',
            13:'FORXMon5',
            14:'FORXMon3',
            15:'FORXMon1',
            }
class FORXServer():
    """
    This class wraps the LabJack and provides a single getStatus method.
    """
    def __init__(self):
        self.lj = u3.U3(localId=5)
        self.lj.setFIOState(4,0) #defaul to input to avoid accidentally rebooting cube
#        self.lj.setFIODir(4,0)
        #self.lj.getCalibrationData()
        
    def setRssCubeReset(self,state=0):
        """
        Set RSS Cube Reset signal
        state = 0 --> give cube power
        state = 1 --> cut power to cube
        """
        if state:
            self.lj.setFIOState(4,1)
            self.lj.setFIODir(4,1)
        else:
            self.lj.setFIOState(4,0)
            time.sleep(1)
            self.lj.setFIODir(4,0)
    def getStatus(self):
        """
        Read the voltages and temperature of the LabJack.
        
        Returns a dictionary of monitor points and values
        """
        stats = {}
    
        channels = channelmap.keys()
        channels.sort()
        ains = [u3.AIN(x,31,True) for x in channels]
        tic = time.time()
        meas = self.lj.getFeedback(ains)
        corelog.debug("got FORX measurements in: %.2f ms" % ((time.time()-tic)*1000))
        for n,ch in enumerate(channels):
            if ch < 4:
                volts = self.lj.binaryToCalibratedAnalogVoltage(meas[n],isLowVoltage = False, isSingleEnded = True)
            else:
                volts = self.lj.binaryToCalibratedAnalogVoltage(meas[n],isLowVoltage = True, isSingleEnded = True)
            stats[ch] = volts*2
            
        fields = {}
        for k,v in stats.items():
            fields[channelmap[k]] = v
        
        fields['RackTemp'] = self.lj.getTemperature() - 273.15 # convert K to C
        return fields



class FORXDBWriter():
    """
    Creates a :class:`~FORXServer` and repeatedly polls the monitor points and stores them in the gavrtdb.
    
    This class should be instantiated, then the :meth:`~loop` method called which loops indefinitely.
    """
    def __init__(self):
        try:
            self.pdb = gavrtdb.GavrtDB(rw=True)
        except:
            corelog.exception("Could not connect to GavrtDB")
        try:
            self.fs = FORXServer()
        except:
            corelog.exception("Could not connect to Fiber Monitor LabJack. Perhaps FORX mon already running?")
    def loop(self):
        while True:
            try:
                data = self.fs.getStatus()
            except:
                corelog.exception("Could not get Fiber Monitor data from LabJack")
                time.sleep(60)
                continue
            
            keystr = ''
            valstr = ''
            values = []
            for k,v in data.items():
                keystr = keystr + k + ', '
                valstr = valstr + '%s, '
                values.append(v)
            keystr = keystr + 'UnixTime'
            valstr = valstr + '%s'
            values.append(time.time())
            try:
                c = self.pdb.cursor()
                c.executemany(
                         "INSERT INTO forx_mon (" + keystr + ") VALUES (" + valstr + ");",
                         [tuple(values)])
                print "INSERT INTO forx_mon (" + keystr + ") VALUES (" + valstr + ");",tuple(values)
                self.pdb.commit()
            except Exception, e:
                corelog.exception("Could not log to GavrtDB, will try reconnecting")
                try:
                    self.pdb = gavrtdb.GavrtDB(rw=True)
                except Exception, e:
                    corelog.exception("Could not reconnect to GavrtDB")
                
            time.sleep(60)
            
if __name__ == "__main__":
    forx = FORXDBWriter()
    corelog.info("Fiber RX Monitor started")
    forx.loop()            