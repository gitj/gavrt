from __future__ import with_statement
import Pyro.core
import Pyro.naming
from Pyro.errors import NamingError
import os

import socket

from measurement import *

RCV_BUFFER_SIZE = 2**24


IBOB_NETWORK = '192.168.0.'
IBOB_BASE_PORT = 59000

class FastIbobCapture(Pyro.core.ObjBase):
    def __init__(self, ibobid):
        Pyro.core.ObjBase.__init__(self)
        
        self.id = ibobid
        self.msgid = 0
        self.running = False
    def run(self):
        
        self.writing = False
        self.running = True
                
        name = ':fastIBOB.'+str(self.id)   
        self.name=name   

        self.iBOB_addr = IBOB_NETWORK + str(self.id+16)

        
        self.packets_received = 0

        self.data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.data_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.data_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCV_BUFFER_SIZE)
        self.data_sock.bind(("", IBOB_BASE_PORT+self.id+16+16))
        
        self.acc = 0
        self.number_of_measurements = 0
        self.data_file = None
        self.accnum_file = None
        self.tstart_file = None

        self.ns = Pyro.naming.NameServerLocator().getNS()
        
        try:
            self.ns.createGroup(':fastIBOB')        #Ensure IBOB group exists
        except NamingError:
            pass
        try:
            uri = self.ns.resolve(name)
            try:
                uri.getProxy().quit()
                print "successfully quit existing daemon"
                time.sleep(2)
            except:
                pass
            try:
                self.ns.unregister(name)
                print "found stale name in ns, and unregistered"
            except NamingError:
                pass
        except NamingError:
            print "No existing daemon registered, good"        
        self.pd = Pyro.core.Daemon()
        self.pd.useNameServer(self.ns)
          
        self.pd.connect(self, name)
        

        print self.name,"starting ibob server"
        
        self.data_sock.setblocking(False) # crucial to avoid deadlock in loop

        while self.running:
            self.pd.handleRequests(1, [self.data_sock], self.processData)
        self.data_sock.close()
        print self.name, "done running"
    def processData(self,ins):
        while True:
            try:
                d = self.data_sock.recv(4096)
            except:
                return
            self.packets_received += 1
            if self.writing:
                measurement_piece = MeasurementPacket(d)
                self.record_measurement(measurement_piece)
    def get_num_packets(self):
        return self.packets_received
    def get_info(self):
        if self.writing:
            msg = "W %d"
        else:
            msg = "%d"
        return (msg % self.get_num_packets())
    def get_id(self):
        return self.id
    def quit(self):
        self.running = False
        self.stop_writing()
        
        try:
            ns = Pyro.naming.NameServerLocator().getNS()
            ns.unregister(self.name)
        except Exception, e:
            print " could not unregister from ns:",e
        
        
    def start_writing(self,dirpath):
        if self.writing:
            self.stop_writing()
        
        self.data_file = open(os.path.join(dirpath,'iBOB%d.spec' % self.id),'wb')
        self.accnum_file = open(os.path.join(dirpath,'iBOB%d.idx' % self.id), 'wb')
        self.tstart_file = open(os.path.join(dirpath,'iBOB%d.start' % self.id), 'w')
        self.writing = True

    def record_measurement(self, measurement):

        if chr(measurement.type) != 'B':
            print "IBOB %d got measurement type %s!" % (self.id,measurement.type)
            return
        if len(measurement.data) != 512:
            print "iBOB",self.id,"Data length:",len(measurement.data),"expected 512"
            return
        
        acc = measurement.master_counter
        if acc - self.acc != 1:
            print "missed:",self.id,acc,self.acc
            if acc <= self.acc:
                print "**** Went backwards or stayed the same!!",self.id
        self.acc = acc
        if acc == 1:
            tstamp = time.time()
            print self.id,": Got pktidx = 1 @",time.ctime(tstamp)
            self.tstart_file.write('PPS packet detected at:\n%f\n\n%s\n' % (tstamp,time.ctime(tstamp)))
            self.tstart_file.flush()
        self.number_of_measurements += 1
        self.data_file.write(measurement.data)
        np.array(measurement.master_counter,dtype='uint32').tofile(self.accnum_file)

    def stop_writing(self):
        self.writing = False
        if self.data_file:
            self.data_file.close()
            self.data_file = None
        if self.accnum_file:
            self.accnum_file.close()
            self.accnum_file = None
        if self.tstart_file:
            self.tstart_file.close()
            self.tstart_file = None

    