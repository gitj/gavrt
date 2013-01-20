"""
:mod:`dss28core.tct_mon`
------------------------

TCT Monitoring program. This module should be automatically run as a standalone program.

Periodically checks the serial time output of the TCT and checks it against the local time. 
The TCT status and time difference are logged in the GavrtDB.
Thus both the TCT status and the NTP time updating of the local machine can both be checked.

The :class:`~TCTServer` is accessible through Pyro as **TCTServer** 
"""
import time
import serial
import calendar
import Pyro.core
import gavrtdb
from loggers import corelog
import config


class TCTServer(Pyro.core.ObjBase):
    """
    Manage the TCT connection monitoring.
    
    The TCT serial port can be explicitly passed in (/dev/ttyUSB*), but by default it will be obtained using the :attr:`~config.serialPortMap` dictionary.
    
    Note the TCT uses 9600 7-Odd-1 format  
    """
    def __init__(self,port=None):
        Pyro.core.ObjBase.__init__(self)
        if port is None:
            try:
                port =config.serialPortMap['TCT']
            except:
                corelog.exception("TCT entry not found in serialPortMap. Manually searching for port")
                port = self.findport()
        corelog.info("Using port %s",port)
        self.ser = serial.Serial(port)
        self.ser.setBaudrate(9600)
        self.ser.setParity('O')
        self.ser.setByteSize(7)
        self.ser.setTimeout(2)
        
        self.ser.flushInput()
        self.running = True
        self.gdb = gavrtdb.GavrtDB(rw=True)
        
    def quit(self):
        """
        Quit the main loop
        """
        self.running = False
    def ping(self):
        """
        Allows remote checking that the Pyro object is still alive
        """
        return True
    def close(self):
        """
        Close the serial port. Not used?
        """
        self.ser.close()
    
    def findport(self):
        """
        Find the TCT. This method is deprecated, but will be used if all other methods fail.
        """
        for pn in range(5):
            print "searching port", pn
            try:
                ser = serial.Serial('/dev/ttyUSB%d' % pn)
            except:
                print "couldnt open", pn
                continue
            self.ser = ser
            # The TCT uses 7-Odd-1 at 9600 bps
            self.ser.setBaudrate(9600)
            self.ser.setParity('O')
            self.ser.setByteSize(7)
            self.ser.setTimeout(2)
            
            self.ser.flushInput()

            tries = 0
            while tries < 2:
                tries += 1
                print "trial",tries
                
                r = self.ser.readline(eol='\r')
                print "response:",r
                if len(r) == 18:
                    print "got response:",pn,r
                    break
            if tries == 2:
                print "no response on",pn
                ser.setParity('N')
                ser.setByteSize(8)
                ser.close()
                continue
            else:
                print "found port:",pn
                ser.close()
                return ('/dev/ttyUSB%d' % pn)
                
    def get(self):
        """
        Get the TCT time string and parse it.
        
        Returns a tuple containing:
        
        * UnixTime stamp as an integer
        * TCT status byte. 0 is good
        * Offset between system time and TCT time in seconds as a float
        """
        self.ser.flushInput()
        txtime = 18*10/9600.    #Amount of time to transmit 18 characters at 9600bps
        r = self.ser.readline(eol='\r')
        now = time.time()-txtime
        if len(r) != 18:
            corelog.warning("TCT did not transmit")
            return None
        #          012345678901234567
        #Format is YYYYjjjHHMMSSrCCC\r
        try:
            Y = int(r[:4]) #year
            doy = int(r[4:7]) #day of year
            H = int(r[7:9]) #hour
            M = int(r[9:11]) #min
            S = int(r[11:13]) #sec
            status = int(r[13])
            checksum = int(r[14:17],16)
        except Exception, e:
            corelog.exception("Couldn't parse TCT output")
            return None
        ut = calendar.timegm(time.strptime("%04d %03d %02d %02d %02d" % (Y,doy,H,M,S), "%Y %j %H %M %S"))
        offset = ut-now
        corelog.debug("Status %d Found ut %f difference from now %f" % (status, ut, offset))
        hold = False
        if status >= 6:
            corelog.warning("TCT in holdover!")
            hold = True
        elif abs(offset) > 1:
            corelog.warning("TCT offset from system clock > 1 second")
        return ut,status,offset
    
    def update(self):
        """
        Get the TCT time and status and insert it in the database.
        """
        res = self.get()
        if res is None:
            return
        ut,status,offset =res
        try:
            c = self.gdb.db.cursor()
            c.execute("INSERT INTO tct_status (UnixTime,Status,Offset) VALUES (%s,%s,%s);",
                      (ut,status,offset))
            self.gdb.db.commit()
        except Exception, e:
            corelog.exception("Couldn't write to database")
            try:
                self.gdb = gavrtdb.GavrtDB(rw=True)
            except Exception, e:
                corelog.exception("Couldn't reconnect to database")


if __name__=="__main__":
    import Pyro.naming
    Pyro.config.PYRO_MULTITHREADED = 0
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('TCTServer')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    tct = TCTServer()
    daemon.connect(tct,"TCTServer")
    while tct.running:
        tct.update()
        daemon.handleRequests(60) #Timeout after 60 seconds, then loop
