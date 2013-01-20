import time

from dbextensions import * 

import numpy as np
from gavrt_constants import ibob_fiber_map

from private import GAVRTDB

def rget(d,la,ld,ch):
    """ return unixtime and voltage for senor at la,ld,ch"""
    msk = np.logical_and(d['LatchAddress']==la,np.logical_and(d['LatchData']==ld,d['AdcChan']==ch))
    return d['UnixTime'][msk],d['Voltage'][msk]



class GavrtDB():
    def __init__(self, rw=False):
        self.rw = rw
        self.connect()
#        self.getMonitorPoints()
    def getMonitorPoints(self):
        self.rssMonPoints = self.get("SELECT * FROM rss_monitor_points")
        npts = len(self.rssMonPoints['ID'])
        rec = np.empty((npts,),dtype=[(x,self.rssMonPoints[x].dtype) for x in self.rssMonPoints.keys()])
        for k,v in self.rssMonPoints.items():
            rec[k][:] = v
        self.rssMonPoints = rec
        self.rssMonPointsDict = {} 
        for mp in self.rssMonPoints:
            key = (mp['LatchAddress'],mp['LatchData'],mp['AdcChan'])
            self.rssMonPointsDict[key] = mp
        
        
    def connect(self):
        rw = self.rw
        try:
            import pymysql as MySQLdb
            import pymysql.converters
            conv_dict = pymysql.converters.conversions.copy()
            conv_dict[pymysql.constants.FIELD_TYPE.DECIMAL] = pymysql.converters.convert_float
            conv_dict[pymysql.constants.FIELD_TYPE.NEWDECIMAL] = pymysql.converters.convert_float
            MySQLdb.converters = pymysql.converters
            _sqlcompress = False # compression not supported by pymysql yet
        except:
            import MySQLdb
            import MySQLdb.converters
            conv_dict = MySQLdb.converters.conversions.copy()
            conv_dict[246] = float  # Convert Decimal fields to float automatically

            _sqlcompress = True

        if rw:
            self.db = MySQLdb.connect(host = GAVRTDB.host, port=GAVRTDB.port ,user=GAVRTDB.write_user,passwd=GAVRTDB.write_passwd,db=GAVRTDB.db,conv=conv_dict,compress=_sqlcompress)
        else:
            self.db = MySQLdb.connect(host = GAVRTDB.host, port=GAVRTDB.port ,user=GAVRTDB.read_user,passwd=GAVRTDB.read_passwd,db=GAVRTDB.db,conv=conv_dict,compress=_sqlcompress)

        self.c = self.db.cursor()
        
    def checkDB(self):
        try:
            self.db.commit()
        except:
            self.connect()
        self.db.commit()
        
    def cursor(self):
        self.checkDB()
        return self.db.cursor()
    
    def commit(self):
        return self.db.commit()
        
    def insertRecord(self,table,rec,keepid=False,update=False):
        self.checkDB()
        return insert_record(self.db, table, rec,keepid = keepid, update = update)
    def getLastId(self,table):
        self.checkDB()
        return get_last_id(self.db,table)
    def getLastRecord(self,table):
        self.checkDB()
        return get_last_record(self.db,table)
    def getRecordById(self,table,id,idname='ID'):
        self.checkDB()
        return get_record_by_id(self.db,table,id,idname=idname)
    def getMonData(self,addr,data):
        self.checkDB()
        self.c = self.db.cursor()
        self.c.execute("""SELECT * FROM rss_mon WHERE `LatchAddress` = %s AND `LatchData` = %s;""",(addr,data))
        res = np.array(self.c.fetchall())
        return res
    
    def get(self,*args):
        self.checkDB()
        return get_as_dict(self.db,*args,**dict(asfloat=True))
    
    def getRec(self,*args):
        self.checkDB()
        return get_as_rec(self.db,*args)
                           
    def getScanStatus(self):
        sc = self.getLastRecord('scans')
        project = self.getRecordById('projects', sc['ProjectID'])['Name']
        observer = self.getRecordById('observers', sc['CurrentObserverID'])['Name']
        sourceid = sc['SourceID']
        if sc['BEE2DataFileID'] != 0:
            sc['BEE2DataFile'] = self.getRecordById('data_files', sc['BEE2DataFileID'])['Path']
        if sourceid == 0:
            source = 'unknown'
        else:
            source = self.getRecordById('`gavrt_sources`.`source`',sourceid,idname='source_id')['name']
        sc['Project'] = project
        sc['Observer'] = observer
        sc['Source'] = source
        
        if sc['StartTime'] == sc['EndTime']:
            sc['InProgress'] = True
        else:
            sc['InProgress'] = False  
        return sc

    def getScan(self,scanid=None,sessionid=None,projectid=None):
        return self.get("SELECT * FROM scans WHERE ProjectID=%s AND Session=%s AND Scan=%s",(projectid,sessionid,scanid))
    
    def getScanTypes(self):
        self.checkDB()
        c = self.db.cursor()
        c.execute("""SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'scans' AND COLUMN_NAME = 'ScanType';""")
        r = c.fetchone()[0]
        return eval(r[4:])
    
    def getPersonality(self,ibob,spss=None):
        assert ibob in range(8)
        self.checkDB()
        if spss is None:
            c = self.db.cursor()
            c.execute("""SELECT UnixTime,iBOBDesignID%d,iBOBADCClock%d FROM spss_config ORDER BY ID DESC LIMIT 1;""" % (ibob,ibob))
            res = c.fetchone()
            descr = [x[0] for x in c.description]
            resd =  dict(zip(descr,res))
        else:
            resd = spss
        clk = float(resd['iBOBADCClock%d' % ibob])
        dsgnid = resd['iBOBDesignID%d' %ibob]
        
        tupd = float(resd['UnixTime'])
        c = self.db.cursor()
        c.execute("""SELECT * FROM ibob_designs WHERE `ID` = %s;""",(dsgnid,))
        res = c.fetchone()
        descr = [x[0] for x in c.description]
        resd =  dict(zip(descr,res))
        pers = resd['Personality']
        
        return (tupd,pers,clk) #update time, personality, adcClock
    
    def getIBOBPersonalities(self,ibobs=range(8),spss=None):
        self.checkDB()
        if spss is None:
            resd = self.get("""SELECT * FROM spss_config ORDER BY ID DESC LIMIT 1;""")
        else:
            resd = spss
        clks = [float(resd['iBOBADCClock%d' % ibob]) for ibob in ibobs]
        dsgnids = [resd['iBOBDesignID%d' %ibob] for ibob in ibobs]
        
        dsgns = self.get("""SELECT * FROM ibob_designs;""")
        
        tupd = float(resd['UnixTime'])
        psltys = []
        for id in dsgnids:
            psltys.append(dsgns['Personality'][dsgns['ID']==id][0])
        
        return (tupd,psltys,clks,dsgnids) 
    
    def getRXStatusByIBob(self,ibob,rss = None):
        if rss is None:
            rss = self.getRSSStatus()
        fib = ibob_fiber_map[ibob]
        chan = rss['Fiber%d' % fib]
        if (chan & 0x01) == 0:
            sb = 1 # upper sideband
        else:
            sb = -1 # lower sidieband
            
        rx = ((chan & 0xC)>>2) + 1
        if (chan & 0x2) == 0:
            pol = 'A'
        else:
            pol = 'B'
        
        syn = float(rss['RX%d_Synth' % rx])
        
        f0 = syn*4 - 22000
        
        feed = rss['RX%d%s_Feed' % (rx,pol)]
        basis = rss['%sPolBasis' % feed]
        return dict(Fiber=fib,Channel=chan,RX=rx, Sideband=sb,Polarization=pol,Synthesizer=syn,f0=f0,Feed=feed, PolarizationBasis=basis)
    
    def getRSSStatus(self):
        self.checkDB()
        crs = self.db.cursor()
        q = r"""SELECT * FROM rss_config ORDER BY ID DESC LIMIT 1;"""
        crs.execute(q)
        vals = list(crs.fetchone())
        keys = [b[0] for b in crs.description]
        rss = dict(zip(keys,vals))
        return rss
    
    def getIBOBStatusAt(self,t,ibob):
        self.checkDB()
        rec = self.get("SELECT * FROM ibob_config WHERE iBOB = %s AND UnixTime < %s ORDER BY ID DESC LIMIT 1;",(ibob,t))
        res = {}
        for k in rec:
            res[k] = rec[k][0]
            if k == 'StatusDict':
                res[k] = eval(rec[k][0])
        return res
    
    def getRSSStatusAt(self,t):
        self.checkDB()
        crs = self.db.cursor()
        q = r"""SELECT * FROM rss_config WHERE ReadyTime < %s ORDER BY ID DESC LIMIT 1;"""
        crs.execute(q,t)
        vals = list(crs.fetchone())
        keys = [b[0] for b in crs.description]
        rss = dict(zip(keys,vals))
        return rss
    
    def getSPSSStatusAt(self,t):
        self.checkDB()
        crs = self.db.cursor()
        q = r"""SELECT * FROM spss_config WHERE UnixTime < %s ORDER BY ID DESC LIMIT 1;"""
        crs.execute(q,t)
        vals = list(crs.fetchone())
        keys = [b[0] for b in crs.description]
        rss = dict(zip(keys,vals))
        return rss
    
    def getSPSSStatus(self):
        self.checkDB()
        crs = self.db.cursor()
        q = r"""SELECT * FROM spss_config ORDER BY ID DESC LIMIT 1;"""
        crs.execute(q)
        vals = list(crs.fetchone())
        keys = [b[0] for b in crs.description]
        spss = dict(zip(keys,vals))
        
        return spss
    
    def getSourceAt(self,t):
        """
        Try to determine what source the antenna was observing at UnixTime t
        
        Returns a dictionary describing the source with name, id, RA, and Dec keys
        """
        import dss28astro
    
        # First look in commanded source table
        res = self.get("SELECT * FROM antenna_cmd WHERE UnixTime < %s ORDER BY ID DESC LIMIT 1",(t,))
        if res:
            source = dict(name=res['Name'][0],RA=res['RA'][0],Dec=res['Dec'][0],id=int(res['SourceID'][0]))
            print "Returning commanded source", source['name']
            return source
        # Failing that, check the antenna position
        antpos = self.get("SELECT * FROM antenna_temp WHERE UnixTime < %s ORDER BY UnixTime DESC LIMIT 1",(t,))
        dt = t - antpos['UnixTime'][0]
        posValid = (dt < 10) #if the entry is less than 10 seconds from the requested time, we call it valid
        Az = antpos['AZ'][0]
        El = antpos['EL'][0]
        mjd = dss28astro.MJD(antpos['UnixTime'][0])[0]
        ra,d = dss28astro.azel_to_radec(Az,El,mjd)
        nearest = self.get("SELECT * FROM gavrt_sources.source ORDER BY ABS(RA - %s) + ABS(`Dec` - %s) LIMIT 1", (ra[0],d[0]))
        nearest = dict(name=nearest['name'][0],RA=nearest['RA'][0],Dec=nearest['Dec'][0],id=int(nearest['source_id'][0]))

        #Also get the source used for the most recent scan
        scan = self.get("SELECT * FROM scans WHERE StartTime < %s ORDER BY ID DESC LIMIT 1",(t,)) 
        if not scan:
            print "no scan"
            return nearest
        if scan['SourceID'][0] == 0:
            if posValid:
                print "No source in scan table, returning nearest source to telescope position",nearest['name']
                return nearest
            else:
                print "Cannot determin source. But if I had to guess..."
                print "requested time",time.ctime(t)
                print "At",time.ctime(antpos['UnixTime'][0]),"Antenna was nearest to",nearest['name']
                next = self.get("SELECT * FROM antenna_temp WHERE ID = %s", (int(antpos['ID'][0]+1),))
                print "Next Record is at",time.ctime(next['UnixTime'][0]),
                Az = next['AZ'][0]
                El = next['EL'][0]
                mjd = dss28astro.MJD(next['UnixTime'][0])[0]
                ra,d = dss28astro.azel_to_radec(Az,El,mjd)
                nextsrc = self.get("SELECT * FROM gavrt_sources.source ORDER BY ABS(RA - %s) + ABS(`Dec` - %s) LIMIT 1", (ra[0],d[0]))
                print "At which point antenna was nearest to",nextsrc['name'][0]
                raise Exception("Could not determine source. No record in scan table, and antenna position is invalid. Antenna position record was %s" % str(antpos))
        else:
            scansource = self.get("SELECT * FROM gavrt_sources.source WHERE source_id = %s", (int(scan['SourceID'][0]),))
            scansource = dict(name=scansource['name'][0],RA=scansource['RA'][0],Dec=scansource['Dec'][0],id=int(scansource['source_id'][0]))
            if posValid:
                if scan['SourceID'][0] == nearest['id']:
                    print "Scan table and antenna position agree", nearest['name']
                    return nearest
                else:
                    if (np.abs(scansource['RA']-nearest['RA']) + np.abs(scansource['Dec']-nearest['Dec'])) < 0.1:
                        print "Scan table and antenna position agree, but use different ids. Source names:",scansource['name'],nearest['name']
                    else:  
                        print "Scan table and antenna position disagree, returning source at antenna position",scansource['name'],nearest['name']
                    return nearest
            print "only have source listed in scan table, so returning that",scansource['name']
            print "Scan started:",time.ctime(scan['StartTime'][0])
            print "Requested time:",time.ctime(t)
            print "Scan Ended:",time.ctime(scan['EndTime'][0])
            return scansource
        
         
    def getPersonalities(self):
        self.checkDB()
        crs = self.db.cursor()
        crs.execute("""SELECT ID,Personality FROM ibob_designs;""")
        vals = list(crs.fetchall())
        pdict = {}
        for v in vals:
            pdict[v[0]] = v[1]
        return pdict
    
    
    def getIPF(self,ipfid):
        self.checkDB()
        crs = self.db.cursor()
        crs.execute("SELECT `IPF`,`Directory` FROM ibob_designs WHERE ibob_designs.ID = %d" % ipfid)
        vals = list(crs.fetchone())
        if len(vals) <1:
            raise Exception("No such IPF_ID")
        keys = [b[0] for b in crs.description]
        resd = dict(zip(keys,vals))
        return resd
    
    def updateValues(self,vald,table):
        """
        Add a row to table with same values as previous row, except for keys in vald, which are updated with provided values
        """
        lastrec = self.getLastRecord(table)
        #print lastrec
        lastrec.update(vald)
        #print lastrec
        self.insertRecord(table, lastrec)
        
