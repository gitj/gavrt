"""
:mod:`dss28core.xant_interface`
-------------------------------

Pyro interface to Xant to provide telescope control

Standalone background program
"""

from __future__ import with_statement
import time
import numpy as np
import os.path
import Pyro.core
import Pyro.core
import Pyro.naming
from Pyro.errors import PyroError, NamingError
import socket
import struct

from gavrtdb import FakeGavrtDB,GavrtDB

from loggers import corelog

from private import XANT_ADDRS

class DummyXantInterface(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
        self.source = {}
        self.gdb = FakeGavrtDB()

    def connect(self,*args,**kwargs):
        print "Dummy: Not Connecting"
    def checkConnected(self):
        return True
    def close(self):
        pass
    def send_cmd(self, msg, timeout=5):
        print "Would have sent:",msg
        return ''

class XantInterface(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
        self._connected = False
        self.addr = None
        self.source = {}
        corelog.info("Connecting to GavrtDB")
        self.gdb = GavrtDB(rw=True)
        rec = self.gdb.get("SELECT * FROM antenna_cmd ORDER BY ID DESC LIMIT 1")
        if rec:
            self.source = dict(name=rec['Name'][0],RA=rec['RA'][0],Dec=rec['Dec'][0],id=int(rec['SourceID'][0]))
            corelog.info("Got most recent source: %s" % (rec['Name'][0]))
        else:
            corelog.error("Could not find a most recent sournce in the antenna_cmd table.")    
    def getAddr(self):
        return self.addr
    def connect(self):
        """ Connect to server running Xant, usually dss28-eac """
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('',9013))
        for addr in XANT_ADDRS:
            tries = 0
            maxtries = 2
            corelog.debug("Trying to connect to xant at %s" % (str(addr)))
            while tries < maxtries:
                tries += 1
                try:
                    self.sock.connect(addr)
                    break
                except Exception, e:
                    corelog.debug("Failed on %d try\n%s" % (tries,str(e)))
            if tries < maxtries:
                corelog.info("Succeeded in connecting to Xant at %s after %d tries" % (str(addr),tries))
                self.sock.setblocking(False)
                self._connected = True
                self.addr = addr
                return addr
        corelog.warning("Failed to connect to Xant")
        self.addr = None
        return None

    def ping(self):
        return True
    def checkConnected(self):
        try:
            self.sock.getpeername()
        except:
            self._connected = False
            self.addr = None
            corelog.warning("Xant not connected")
            return False
        corelog.debug("Xant is connected")
        self._connected = True
        return True
        
    def close(self):
        self.sock.close()
        
    def _send(self,msg,magic=0x00050001):
        hdr = struct.pack('!IIII',0x3C54543E,len(msg)+8+1,magic,0)
        self.sock.sendall(hdr+msg+'\x00')

    def send_cmd(self,cmd,timeout=5):
        """
        Send a command to XANT and get response.
        """
        if not self.checkConnected():
            self.connect()
        
        while True:
            try:
                self.sock.recv(10000)
            except:
                break
        
        tic = time.time()
        try:
            self._send(cmd)
        except Exception,e:
            corelog.exception("Couldn't send command to XANT")
            return
        tstart = time.time()
        resp = ''
        while (time.time()-tstart < timeout):
            try:
                resp = resp + self.sock.recv(1000)
                if resp.find('completed') >= 0:
                    corelog.debug("Command %s accepted" % cmd)
                    break
                elif resp.find('rejected') >= 0:
                    corelog.warning("Command %s rejected with response: %s" % (cmd,resp))
                    break
        
            except:
                corelog.exception("Problem waiting for response from XANT")
                pass
        corelog.debug("Xant response time: %.2f ms" %((time.time()-tic)*1000))
        time.sleep(0.1) #sleep a moment 
        return resp

def runXantInterface():    
    import Pyro.naming
    
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('XantInterface')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    rssi = XantInterface()
    rssi.connect()
    daemon.connect(rssi,"XantInterface")
    daemon.requestLoop()
    
def runDummyInterface():
    import Pyro.naming
    
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('DummyXantInterface')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    obj = DummyXantInterface()
    obj.connect()
    daemon.connect(obj,"DummyXantInterface")
    daemon.requestLoop()
if __name__=="__main__":
    import sys
    if len(sys.argv) > 1:
        runDummyInterface()
    else:
        runXantInterface()