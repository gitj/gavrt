"""
:mod:`dss28core.powerdbwriter`
------------------------------

Reads status from the Lambda Power supply (provides +5 V to IBOBs and BEE2) and stores in the GavrtDB
"""
import gavrtdb
import time
import serial

from loggers import corelog
import config

fields = {
          'UnixTime':time.time,
          'SetVoltage': lambda : 'PV',
          'SetCurrent': lambda : 'PC',
          'MeasuredVoltage': lambda : 'MV',
          'MeasuredCurrent': lambda : 'MC',
          'Status' : lambda : 'SR',
          'Fault' : lambda : 'FR'
          }

class PowerDBWriter():
    def __init__(self):
        self.ps = SPSSPowerServer()
        try:
            self.pdb = gavrtdb.GavrtDB(rw=True)
        except Exception, e:
            corelog.exception("Could not connect to database")
    def loop(self):
        while True:
            data = self.ps.getStatus()

            keystr = ''
            valstr = ''
            values = []
            for k,v in fields.items():
                keystr = keystr + k + ', '
                valstr = valstr + '%s, '
                val = v()
                if data.has_key(val):
                    val = data[val]
                values.append(val)
            keystr = keystr[:-2]    #strip final ,
            valstr = valstr[:-2] 
            try:
                c = self.pdb.cursor()
                c.executemany(
                         "INSERT INTO spss_power (" + keystr + ") VALUES (" + valstr + ");",
                         [tuple(values)])
                print "INSERT INTO spss_power (" + keystr + ") VALUES (" + valstr + ");" , values
                self.pdb.commit()
            except Exception, e:
                corelog.exception("Error inserting power info")
                try:
                    self.pdb = gavrtdb.GavrtDB(rw=True)
                except Exception, e:
                    corelog.exception("Could not reconnect to database")
            time.sleep(60)
            
            
class SPSSPowerServer():
    """
    Power supply serial port is obtained from :mod:`config` serialPortMap
    """
    def __init__(self, port=None):
        if port is None:
            try:
                port =config.serialPortMap['Power']
            except:
                corelog.exception("Power entry not found in serialPortMap. Manually searching for port")
                port = self.findport()
        corelog.info("Using port %s",port)
        self.ser = serial.Serial(port)
        self.ser.setParity('N')
        self.ser.setByteSize(8)
        self.ser.setBaudrate(9600)
        self.ser.setTimeout(0)
    def close(self):
        self.ser.close()
    
    def findport(self):
        """
        Deprecated method to find the power supply. Will be used if no other option to find the serial port
        """
        for pn in range(4):
            print "searching port", pn
            try:
                ser = serial.Serial('/dev/ttyUSB%d' % pn)
            except:
                print "couldnt open", pn
                continue
            self.ser = ser
            self.ser.setBaudrate(9600)
            self.ser.setTimeout(0)

            tries = 0
            while tries < 5:
                tries += 1
                print "trial",tries
                
                r = self.sendget('adr 6')
                print "response:",r
                if r.find('OK') >= 0:
                    print "got response:",pn,r
                    break
            if tries == 5:
                print "no response on",pn
                ser.close()
                continue
            else:
                print "found port:",pn
                ser.close()
                return ('/dev/ttyUSB%d' % pn)
                
    def sendget(self,cmd):
        """
        Send serial command and get response
        """
        corelog.debug("Sending: %s" % cmd)
        self.ser.flushInput()
        self.ser.setTimeout(1)
        self.ser.write(cmd+'\r')
        r = self.ser.readline(eol='\r')
        corelog.debug("Received: %s" % r)
        return r
    def getStatus(self):
        """
        Request status and parse response
        """
        stats = {}
        try:
            r = self.sendget('stt?')
            fields = r.strip().split(',')
            for f in fields:
                parts = f.split('(')
                value = parts[1][:-1]
                if len(value) > 2:
                    stats[parts[0]] = float(value)
                else:
                    stats[parts[0]] = int(value,16)
        except:
            corelog.exception("Could not parse power supply output!")
        return stats
    def checkConnected(self):
        self.ser.flushInput()
        self.ser.write('adr 6\r')
        time.sleep(0.1)
        r = self.ser.read(100)
        if r.find('OK') >= 0:
            print "found",r
            return True
        else:
            print "didn't find",r
            return False
if __name__ == "__main__":
    pdw = PowerDBWriter()
    pdw.loop()