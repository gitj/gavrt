"""
:mod:`dss28core.rss_interface`
------------------------------

Low level connection to Receiver Control Terminal (RCT) program which runs on dss28-eac.

This module is intended to automatically run standalone to provide a Pyro Object **RSSInterface** to broker access to the RCT.

Provides control of receiver subsystem to the :class:`~rss.RSS` class.

Commands are documented on wiki at 
"""
from __future__ import with_statement
import time
import os.path
import Pyro.core
import Pyro.naming
import socket
import struct

from loggers import corelog

from private import RCT_ADDRS #Possible places the RCT could be running:

class RSSInterface(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
        self._connected = False
    def connect(self):
        """
        Open a TCP connection to the RCT
        """
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('',9002))
        for addr in RCT_ADDRS:
            tries = 0
            while tries < 1:
                try:
                    self.sock.connect(addr)
                    corelog.info("RCT Connected at %s" % (str(addr)))
                    break
                except Exception, e:
                    corelog.info("Could not connect to RCT on %s" % (str(addr)))
                    tries += 1
            if tries <1:
                self.sock.setblocking(False)
                self._connected = True
                return
            else:
                corelog.error("Could not connect to RCT on %s" % (str(addr)))
                self._connected = False
    def checkConnected(self):
        """
        Check that connection is still available to RCT.
        
        Returns bool
        """
        print "checking connection"
        try:
            self.sock.getpeername()
        except:
            self._connected = False
            self.addr = None
            corelog.warning("RSS not connected")
            return False
        corelog.debug("RSS is connected")
        self._connected = True
        return True

    def ping(self):
        return True
    def close(self):
        self.sock.close()
        
    def send(self,msg,magic=0x00050001):
        """
        Wraps a message in the <TT> format for the RCT
        """
        hdr = struct.pack('!IIII',0x3C54543E,len(msg)+8+1,magic,0)
        self.sock.sendall(hdr+msg+'\x00')

    def send_cmd(self,cmd,timeout=5):
        """
        Main function for sending a commaand string to the RCT and receiving a response.
        """
        try:
            self.sock.getpeername()
        except:
            corelog.warn("Trying to send command but found not connected, so trying to connect")
            self.connect()
        
        while True:
            try:
                self.sock.recv(10000)
            except:
                break
        
        tic = time.time()
        try:
            self.send(cmd)
            corelog.debug("Sent command %s" % cmd)
        except Exception,e:
            corelog.exception("Could not send command %s" % cmd)
            return
        tstart = time.time()
        resp = ''
        while (time.time()-tstart < timeout):
            try:
                resp = resp + self.sock.recv(1000)
                if resp.find('COMPLETED:') >= 0:
                    corelog.debug("RSS successfully executed command %s in %.2f ms" % (cmd,(time.time()-tic)*1000))
                    time.sleep(0.1) #sleep a moment to ensure receiver is setup
                    return resp
        
            except:
                pass
        return resp
    
if __name__=="__main__":
    import Pyro.naming
    
    Pyro.core.initServer()
    ns = Pyro.naming.NameServerLocator().getNS()
    try:
        ns.unregister('RSSInterface')
    except:
        pass
    daemon = Pyro.core.Daemon()
    daemon.useNameServer(ns)
    rssi = RSSInterface()
    rssi.connect()
    daemon.connect(rssi,"RSSInterface")
    daemon.requestLoop()