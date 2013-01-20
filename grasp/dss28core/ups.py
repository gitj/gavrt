"""
Routines to retrieve and plot data from Spectrometer UPS
"""
import os
import numpy as np
from ftplib import FTP
import tempfile
import datetime
import dateutil.parser
import time
import gavrtdb
import calendar #timegm
from loggers import corelog,DSS28

from private import SPSS_UPS

class UPS():
    def __init__(self, old = None,ipaddr=SPSS_UPS.host):
        if old:
            self.data = old.data
            self.gdb = old.gdb
        else:
            self.data = None
            self.gdb = gavrtdb.GavrtDB(rw=True)
            
        self.addr = ipaddr
    
    def getData(self,passwd=SPSS_UPS.passwd):
        """
        Retrieve data via FTP from UPS
        """
        if passwd is None:
            print "Enter password for root on UPS: ",
            passwd = raw_input()
            
        corelog.info("Retrieving data from UPS")
        try:
            self.ftp = FTP(self.addr)
            self.ftp.login(user=SPSS_UPS.user,passwd=passwd)
        except Exception, e:
            corelog.exception("Couldn't connect to UPS by FTP at address %s with user root and password %s" % (self.addr,passwd))
                
        fname = time.strftime(os.path.join(DSS28,'logs/ups/%Y.%j_%H%M%S_ups.txt'))
        try:
            dfile = open(fname,'w')
        except Exception, e:
            corelog.exception("Could not open file %s for writing UPS data" % fname)
        try:
            self.ftp.retrbinary("RETR data.txt", dfile.write)
        except Exception, e:
            corelog.exception("Could not retrieve UPS data file data.txt")
        dfile.close()
        self.ftp.close()

        self.parseData(dfile.name)

        
    def parseData(self,fname):
        from matplotlib import mlab
        corelog.debug("Parsing UPS data file: %s",fname)
        def myfloat(x):
            try:
                return float(x)
            except:
                return 0.0
        names = ['date','time','vmin','vmax','vout','wout','freq','cap','vbat','tupsc']
        convd = {}
        for k in names:
            convd[k] = myfloat
        dp = dateutil.parser.parse
        convd['date'] = dp
        convd['time'] = dp
        try:
            res = mlab.csv2rec(fname,skiprows=6,names=names,converterd = convd,delimiter='\t')
        except Exception,e:
            corelog.exception("Could not parse UPS data file")
        self.data = res
        corelog.info("Retrieved %d UPS data values" % (self.data.shape[0]))
        try:
            self.convTime()
        except Exception,e:
            corelog.exception("Failed to convert UPS time data")
        
    def insertData(self):
        if self.data is None:
            print "no data!"
            return
        corelog.info("Inserting UPS data: %d values" % (self.data.shape[0]))

        ut = [calendar.timegm(x.timetuple()) for x in self.data['time']]
        data = self.data
        try:
            c = self.gdb.cursor()
            for k in range(len(ut)):
                data = self.data[k]
                c.execute("INSERT IGNORE INTO spss_ups (UnixTime,Vmin,Vmax,Vout,Wout,Freq,Capacity,Vbat,Temperature) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                          (ut[k],data['vmin'],data['vmax'],data['vout'],data['wout'],data['freq'],data['cap'],data['vbat'],data['tupsc']))
            self.gdb.commit()
        except Exception, e:
            corelog.exception("Couldn't write UPS data to database")
            try:
                self.gdb = gavrtdb.GavrtDB(rw=True)
            except Exception, e:
                corelog.exception("Couldn't reconnect to database")
        corelog.info("Finished inserting UPS data")

    def convTime(self):
        res = self.data
        for x in range(res.shape[0]):
            dt = res[x]['date']
            tm = res[x]['time']
            res[x]['time'] = datetime.datetime(dt.year,dt.month,dt.day,tm.hour,tm.minute,tm.second)
            
        self.data = res

    def loop(self,interval=3600):
        while True:
            corelog.info("Getting UPS data and inserting in database...")
            try:
                self.getData()
                self.insertData()
            except:
                corelog.exception("Failed to get UPS data and insert to database")
            corelog.debug("UPSLogger sleeping for %d seconds" % interval)
            time.sleep(interval)
        
    def makePlots(self,fname = None):
        from matplotlib import pyplot as plt
        f = plt.figure()
        ax1 = f.add_subplot(311)
        ax2 = f.add_subplot(312,sharex=ax1)
        ax3 = f.add_subplot(313,sharex=ax1)
        
        data = self.data
        t = data['time']
        ax1.plot(t, data['vmax'],'r',label='Vmax')
        ax1.plot(t, data['vmin'],'b',label='Vmin')
        ax1.plot(t, data['vout'],'g',label='Vout')
        ax1.set_ylim(170,220)
        ax1.set_ylabel('Volts')
        ax1.grid()
        ax1.legend(loc='lower left',prop=plt.matplotlib.font_manager.FontProperties(size='x-small'))
        
        ax2.plot(t, data['cap'],label='Battery Capacity (%)')
        ax2.plot(t, data['vbat'],label='Battery Voltage (V)')
        ax2.plot(t, data['wout'],label='Output Load (%)')
        ax2.set_ylim(ymax=110)
        ax2.grid()
        ax2.legend(loc='lower left',prop=plt.matplotlib.font_manager.FontProperties(size='x-small'))
        
        ax3.plot(t, data['tupsc'],label='Temperature (degC)')
        ax3.grid()
        ax3.legend(loc='lower left',prop=plt.matplotlib.font_manager.FontProperties(size='x-small'))
        f.autofmt_xdate()
        ax3.set_xlim(ax3.axis('tight')[:2])
        
        if fname:
            f.savefig(fname+'.png')
            sortt = t.copy()
            sortt.sort()
            ax3.set_xlim(sortt[-200],sortt[-1])
            f.savefig(fname+'_zoom.png')
        
        
if __name__ == "__main__":
    u = UPS()
    u.loop()
    