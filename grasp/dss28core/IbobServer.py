"""
:mod:`dss28core.IbobServer`
----------------------------

Class which provides UDP communications with an iBOB. Provides:

    * Monitor and control communications (configure registers etc.)
    * Constant real-time data buffer for plotting and monitoring
    * Ability to write data to hdf5 archive file for offline analysis
"""

from __future__ import with_statement
import Pyro.core
import Pyro.naming
from Pyro.errors import NamingError
import numpy as np
import time
import tables 
import os

import socket
import struct

from measurement import *
import personalities

from multiprocessing import Lock

from loggers import corelog

MAX_MEASUREMENTS_IN_PROGRESS = 20

MAX_REALTIME_ROWS = 1024
MAX_CHARS_PER_COMMENT = 512

RCV_BUFFER_SIZE = 2**24

SENDGET_ATTEMPTS = 10

IBOB_NETWORK = '192.168.0.'
IBOB_BASE_PORT = 59000

class IbobServer(Pyro.core.ObjBase):
    def __init__(self, ibobid):
        Pyro.core.ObjBase.__init__(self)
        
        self.id = ibobid
        self.msgid = 0
        self.running = False
    def run(self):
        
        self.writing = False
        self.running = True
        self.personality = None
        
        self.realtime_lock = Lock()
        self.h5_lock = Lock()
        
        name = ':IBOB.'+str(self.id)   
        self.name=name   

        self.iBOB_addr = IBOB_NETWORK + str(self.id+16)


        self.control_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.control_sock.setblocking(False)
        self.control_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 7 is the port on which the iBOB listens for commands
        self.control_sock.connect((self.iBOB_addr, 7))

        self.measurements_dict = {}
        self.measurements_list = []
        self.spec_info_table = None
        self.realtime_infotable = None
        
        self.packets_received = 0

        self.data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.data_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.data_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCV_BUFFER_SIZE)
        self.data_sock.bind(("", IBOB_BASE_PORT+self.id+16))
        
        self.acc = 0
        self.number_of_measurements = 0
        self.h5 = None
        self.realtime_h5 = None

        self.ns = Pyro.naming.NameServerLocator().getNS()
        
        try:
            self.ns.createGroup(':IBOB')        #Ensure IBOB group exists
        except NamingError:
            pass
        try:
            uri = self.ns.resolve(name)
            try:
                uri.getProxy().quit()
                corelog.debug("successfully quit existing daemon for ibob %d" % self.id)
                time.sleep(2)
            except:
                pass
            try:
                self.ns.unregister(name)
                corelog.debug("found stale name in ns, and unregistered for ibob %d" % self.id)
            except NamingError:
                pass
        except NamingError:
            corelog.debug("No existing daemon registered, good ibob %d" % self.id)        
        self.pd = Pyro.core.Daemon()
        self.pd.useNameServer(self.ns)
          
        self.pd.connect(self, name)
        

        corelog.info("Starting %s server" % self.name)
        
        self.data_sock.setblocking(False) # crucial to avoid deadlock in loop

        while self.running:
            self.pd.handleRequests(1, [self.data_sock], self.processData)
        self.data_sock.close()
        corelog.info("%s server done running" % self.name)

    def processData(self,ins):
        while True:         #adding this while loop (to process all pending packets) did not improve speed 2010.09.10
            try:
                d = self.data_sock.recv(4096)
            except:
                return
            self.packets_received += 1
            measurement_piece = MeasurementPacket(d)
            self.reassemble_measurement(measurement_piece)
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
    def ping(self):
        return True
    def quit(self):
        self.running = False
        self.stop_writing()
        with self.realtime_lock:
            if self.realtime_h5:
                self.realtime_h5.close()
                self.realtime_h5 = None
                self.realtime_filename = None
        try:
            ns = Pyro.naming.NameServerLocator().getNS()
            ns.unregister(self.name)
        except Exception, e:
            corelog.warning("%s could not unregister from ns: %s" %(self.name,str(e)))
        
    def clear_personality(self):
        corelog.debug("%s Clearing personality" % self.name)
        self.personality = None
        with self.realtime_lock:
            if self.realtime_h5:
                self.realtime_h5.close()
                self.realtime_h5 = None
                corelog.debug("%s Closing realtime h5" % self.name)
            
    def set_personality(self, personality,adcClock=1024.0):
        """
        sets the personality of a specific iBOB
        """
        if self.writing or self.personality:
            corelog.warning("%s cannot set personality: it's already set" % self.name)
            return
        corelog.debug("%s Setting personality %s %s" % (self.name,str(personality), personalities.__file__))
        self.personality = personality(adcClock=adcClock)  # removed self as parent
        try:
            self._init_rtbuf()
        except Exception,e:
            corelog.exception("Could not init realtime buf for ibob %d %s"%(self.id,str(self.personality)))

    def get_personality(self):
        return self.personality
        
        
    def reassemble_measurement(self, piece):
        # look for the right Measurement to add this Piece to
        # if we found it, add it to that Measurement
        piece_key = str(piece)  #str method is overridden so this is efficient and unique
        if piece_key in self.measurements_dict.keys():
            m = self.measurements_dict[piece_key]
            m.place_piece(piece)
        else:
            # packet was not in any measurement, so make a new one
            new_measurement = Measurement(piece)

            self.measurements_dict[str(new_measurement)] = new_measurement
            self.measurements_list.append(str(new_measurement))
        
        # check to find Measurements that are complete
        # only one Measurement could possibly be completed (the one that the
        # current Piece fits into)
        measurement = self.measurements_dict[piece_key]
        if measurement.is_complete():
            
            # sends Measurement to personality to parse the raw BRAM data,
            # then sends the parsed data to the data server for writing
            try:
                self.record_measurement(measurement)
                #        self.personality._reconstructMeasurement(measurement))
            except Exception, e:
                corelog.exception("%s Could not record measurement" % self.name)

            # remove it from list of Measurements
            self.measurements_list.remove(piece_key)
            del self.measurements_dict[piece_key]

        # Throttle: if the number of Measurements exceeds some value x, delete
        # the first x/2 number of Measurements
        if len(self.measurements_list) > MAX_MEASUREMENTS_IN_PROGRESS:
            corelog.warning("%s too many incomplete measurements, dropping half" % self.name)
            delete_this = self.measurements_list[:MAX_MEASUREMENTS_IN_PROGRESS / 2]
            for m in delete_this:
                del self.measurements_dict[m]
            del self.measurements_list[:MAX_MEASUREMENTS_IN_PROGRESS / 2]

    def get_measurements(self):
        return self.measurements_dict
    
    
    def _init_rtbuf(self):
        self.realtime_filename = "/tmp/rt%d.h5" % self.id
        corelog.info("Starting realtime data capture. Creating %s" %self.realtime_filename)
        # try to open the realtime h5 file for writing
        with self.realtime_lock:
            try:
                self.realtime_h5 = tables.openFile(self.realtime_filename, "w")
            except Exception, e:
                corelog.exception("could not open new realtime h5 file for writing: %s" % self.realtime_filename)
                self.realtime_h5 = None
                self.realtime_filename = None
                return
            
            # if no iBOBs are registered, error and quit
            personality = self.personality
            iBOB_group = self.realtime_h5.root
            
            self.realtime_h5.createTable(iBOB_group, "file_info", dict(personality=tables.StringCol(128,dflt=' ')))
            iBOB_group.file_info.row['personality'] = self.personality.__class__.__name__
            iBOB_group.file_info.row.append()
            iBOB_group.file_info.flush()
            
            corelog.debug("set personality: %s" % str(iBOB_group.file_info[:]))
            
            self.realtime_iBOB_group = iBOB_group
            
            self.realtime_measurements = {}
            iBOB_meas = self.realtime_measurements
            
            measurement_types = personality._measTypesDict
            for measurement_type in measurement_types.keys():
                corelog.debug("adding measurement type: %s" % measurement_type)
                iBOB_meas[measurement_type] = {}
                meas_grp = self.realtime_h5.createGroup(iBOB_group,measurement_type)
                iBOB_meas[measurement_type]['group'] = meas_grp
                thistable = \
                    self.realtime_h5.createTable(meas_grp, 'table',
                                                 measurement_types[measurement_type]['table'],
                                                 expectedrows=2000)
                iBOB_meas[measurement_type]['table'] = thistable
                iBOB_meas[measurement_type]['arrays'] = {}
                iBOB_meas[measurement_type]['index'] = self.realtime_h5.createArray(meas_grp, 'index', np.zeros((1,),dtype='uint32'))
                for name,shape in measurement_types[measurement_type]['arrays'].items():
                    if (name.lower().find('adc') >= 0):
                        fullshape = tuple([16]+list(shape)) # kludge to reduce wasted space on lots of adc snapshots
                    elif shape[0] > 1024:
                        fullshape = tuple([2**20/shape[0]]+list(shape)) #keep size = 1Mpoint
                    else:
                        fullshape = tuple([MAX_REALTIME_ROWS]+list(shape))
                    thisarr = self.realtime_h5.createArray(meas_grp, name, np.zeros(fullshape,dtype='float32'))
                    iBOB_meas[measurement_type]['arrays'][name] = thisarr
    
            self.realtime_infotable = \
                    self.realtime_h5.createTable(iBOB_group, "InfoTable",
                            personality._infoTable, expectedrows = 2000)
                    
        corelog.debug("Finished realtime data capture init %s" % self.name)

    def start_writing(self,filename):
        self.prepare_for_writing(filename)
        self.writing = True
    def prepare_for_writing(self, filename):
        """
        prepares the real h5 (history) file to be written
        """

        
        self.filename = filename
        if os.path.isfile(filename):
            corelog.warning("%s  h5 filename %s already exists, overwriting..." % (self.name,filename))
        
        with self.h5_lock:
            # try to open the file for writing
            try:
                self.h5 = tables.openFile(self.filename,'w')
            except Exception, e:
                self.h5 = None
                self.filename = None
                corelog.exception("%s could not open new h5 file for writing: %s" % (self.name,self.filename))
                return
            
            self.comment_table = self.h5.createTable(self.h5.root, "comment_table",
                        self.comment_table_description())
            # set up tables for each iBOB based on each iBOB's personality
            personality = self.personality
            iBOB_group = self.h5.root
            
            self.h5.createTable(iBOB_group, "file_info", dict(personality=tables.StringCol(128,dflt=' ')))
            iBOB_group.file_info.row['personality'] = self.personality.__class__.__name__
            iBOB_group.file_info.row.append()
            self.h5.flush()
    
            self.iBOB_group = iBOB_group
            
            self.measurements = {}
            iBOB_meas = self.measurements
            
            measurement_types = personality._measTypesDict
            for measurement_type in measurement_types.keys():
                iBOB_meas[measurement_type] = {}
                meas_grp = self.h5.createGroup(iBOB_group,measurement_type)
                iBOB_meas[measurement_type]['group'] = meas_grp
                thistable = \
                    self.h5.createTable(meas_grp, 'table',
                                                 measurement_types[measurement_type]['table'],
                                                 expectedrows=2000)
                iBOB_meas[measurement_type]['table'] = thistable
                iBOB_meas[measurement_type]['arrays'] = {}
                for name,shape in measurement_types[measurement_type]['arrays'].items():
                    fullshape = tuple([0]+list(shape))
                    thisarr = self.h5.createEArray(meas_grp, name, 
                                                   tables.Float32Atom(),fullshape)
                    iBOB_meas[measurement_type]['arrays'][name] = thisarr
    
            self.spec_info_table = self.h5.createTable(iBOB_group, "InfoTable",
                                personality._infoTable, expectedrows = 2000)

        corelog.debug("Finished creating h5 for writing %s" % self.name)
    

    def record_measurement(self, measurement):
        """
        save data to realtime h5 file for recent access, save to history file if 
        necessary

        spec_measurement is a tuple from the spectrometer personality,
        [0] = name
        [1] = arraydict
        [2] = tabledict
        """
#        print "start of record measurement",iBOB_id
        personality = self.personality
        if personality is None:
            return
        spec_measurement = personality._reconstructMeasurement(measurement)
        measurement_type = spec_measurement[0]
        arrays = spec_measurement[1]
        table_data = spec_measurement[2]
#        print measurement_type, len(table_data),table_data
        if measurement_type == 'S':
            acc = table_data['AccNumber']
            if acc - self.acc != 1:
                print "missed:",self.id,acc,self.acc
            self.acc = acc
        #self.publish('msr', spec_measurement)
        self.number_of_measurements += 1
        # always write to realtime h5 file
        
#        table_string = "%s_%s" % (measurement_type, "table_description")
        if not self.realtime_h5:
            corelog.warning("%s no realtime_h5 file opened yet, record_measurement failed" % self.name)
            return
        with self.realtime_lock:
            table = self.realtime_measurements[measurement_type]['table']
            if len(table) >= MAX_REALTIME_ROWS:
                # TODO: change this to not delete just one row
                table.removeRows(0)
                # TODO: hopefully pytables reassigns the indices of table.row, otherwise
                # we have to adjust...
            for key in table_data.keys():
                try:
                    table.row[key] = table_data[key]
                except Exception, e:
                    corelog.exception("%s could not insert data for key %s" %(self.name,str(key)))
                #print "added data for ",key
            try:
                table.row.append()
            except Exception, e:
                corelog.exception("%s could not append row to realtime h5 file" % self.name)
            #print "appended row"
            table.flush()
            #print "added to table"
            rtarrays = self.realtime_measurements[measurement_type]['arrays']
            index = self.realtime_measurements[measurement_type]['index'][0]
            for array_name in arrays.keys():
                rtarray = rtarrays[array_name]
                array_data = arrays[array_name]
                # if too long, do wraparound, index is stored in 
                # table_dict[array_string + "_index"]
                try:
                    rtarray[index] = array_data[np.newaxis,:]
                except Exception, e:
                    corelog.exception("%s arrays have incompatible shapes %s shape is %s. Tried to append %s" % (self.name, array_name, rtarray[index].shape,array_data[np.newaxis,:].shape))
                    raise e
            self.realtime_measurements[measurement_type]['index'][0] = \
                    (index + 1) % rtarrays[arrays.keys()[0]].shape[0]
            self.realtime_h5.flush()
                # array.removeRows(0, 1)
#            print "added to array"
        # only write to real history h5 file if we should
        if self.writing:
#            print table_data.keys(),arrays.keys()
            if not self.h5:
                corelog.warning("%s we are writing but no h5 file opened yet??, record_measurement failed" % self.name)
                return
            with self.h5_lock:
                table = self.measurements[measurement_type]['table']
                for key in table_data.keys():
                    try:
                        table.row[key] = table_data[key]
                    except Exception, e:
                        corelog.exception("%s could not insert data for key %s" %(self.name,str(key)))
#                    print "added data for ",key
                try:
                    table.row.append()
                except Exception, e:
                    corelog.exception("%s could not append row to h5 file" % self.name)
                #print "appended row"
                table.flush()
                #print "added to table"
                rtarrays = self.measurements[measurement_type]['arrays']
                for array_name in arrays.keys():
                    rtarray = rtarrays[array_name]
                    array_data = arrays[array_name]
                    # if too long, do wraparound, index is stored in 
                    # table_dict[array_string + "_index"]
                    
                    rtarray.append(array_data[np.newaxis,:])


    # ======================================
    # Control functions
    # ======================================
    
    def sendget_robust(self,message):
        """
        Send and receive control commands to iBOB using robust UDP protocol
        """
        header_fmt = '>IHBB'
        self.control_flush()
        
        
        tstart = time.time()
        while time.time() - tstart < 1.0:
            msgid = (0xFF<<24)+self.msgid
            self.msgid += 1
            if self.msgid > 0xFFFF00:
                self.msgid = 0
            hdr = struct.pack(header_fmt,msgid,0,0,0)
            msg = hdr + message
            self.control_send_msg(msg)
            tsend = time.time()
            resp = ''
            nextseq = 0
            while time.time() - tsend < 0.2:
                r = self.readone()
                if len(r) >= 8:
                    #print "waiting for seq:",nextseq,("msgid:%08X" %msgid)
                    rxid,seq,type,blah = struct.unpack(header_fmt,r[:8])
                    #print ("\nrxid:%08X"%rxid),"seq",seq,"type",type
                    if rxid == msgid:
                        if seq == nextseq:
                            resp += r[8:]
                            if type == 2:
                                corelog.debug("%s cmd '%s' response in %.2f ms: '%s'" %(self.name,msg,(time.time()-tstart)*1000,resp))
                                return resp
                            else:
                                nextseq += 1
                                continue
                        else:
                            corelog.debug("%s ibob sequence error: expected seq: %d got %d" % (self.name,nextseq,seq,r))
                    else:
                        corelog.debug("%s expected msgid %08X but got %08X" % (self.name,msgid,rxid))
                elif len(r):
                    corelog.debug("%s received: %s" % (self.name,r))
                    
                            
            
                
    def readone(self):
        try:
            read = self.control_read()
            if read is None:
                return ''
            else:
                return read
        except:
            return ''
        
        
    def sendget_command(self,message):
        return self.sendget_robust(message)
    
    def control_send_msg(self, cmd):
        """
        internal: sends a message to the iBOB's command port
        """
        self.control_sock.sendall(cmd + "\n");

    def control_flush(self):
        """
        internal: flushes all read data from the iBOB's command port's read
        """
        try:
            while self.control_sock.recv(4096):
                pass
        except:
            pass

    def control_read(self):
        """
        internal: tries to read a message from the iBOB's command port's read
        returns None if nothing read
        """
        try:
            return self.control_sock.recv(4096)
        except:
            return None

    def write_register(self, register, value):
        """
        given a register and a value, sends an iBOB command to set that register to
        that value, and checks for correct response on read
        """
        read = self.sendget_command("regwrite %s 0x%x" % (register, value))
        if read != "\r":
            print "Error: incorrect output read after sending command: regwrite %s 0x%x" \
                    % (register, value)
        
        # TODO: possibly add code to log this action in debug mode (have a column
        # that stores as strings the commands run)

    def read_register(self, register):
        """
        given a register, tries to read the value of the register
        """
        resp = self.sendget_command("regread %s" % register)
        try:
            st = resp.find('0x')
            r = resp[st:st+10]
            r = int(r,16)
            return r
        except:
            print "received",resp,"could not parse"
            return None


            

    def comment_table_description(self):
        """
        this should really be a constant, but it just returns the comment table description
        """
        return {
                "UserID" : tables.StringCol(20, dflt="User"),
                "Comment" : tables.StringCol(MAX_CHARS_PER_COMMENT),
                "Timestamp" : tables.Float64Col()
               }

    def write_comment(self, user_id, comment):
        with self.h5_lock:
            if not self.h5:
                print "Error: no h5 file open, cannot write comment"
                return
            self.comment_table.row["userID"] = user_id
            self.comment_table.row["comment"] = comment
            self.comment_table.row.append()
            self.comment_table.flush()


    def write_spec_info(self, spec_info_dict):
        """
        write the given spec_info_dict as spec data changes to the spec_info table
        """
        
        table = self.realtime_infotable
        print "write_spec_info"
        with self.realtime_lock:
            print "got lock"
            if self.realtime_h5:
                print "rt5 exists"
                for attribute in spec_info_dict.keys():
                    print "writing",attribute
                    table.row[attribute] = spec_info_dict[attribute]
                    table.row["Timestamp"] = time.time()
                    
                table.row.append()
                table.flush()
                
        table = self.spec_info_table
        
        with self.h5_lock:
            if not self.h5:
                print "Error: no h5 file open, cannot write spec info"
                return
            for attribute in spec_info_dict.keys():
                table.row[attribute] = spec_info_dict[attribute]
                table.row["Timestamp"] = time.time()
            table.row.append()
            table.flush()
    

    def stop_writing(self):
        self.writing = False
        if self.h5:
            with self.h5_lock:
                self.h5.close()
                self.h5 = None
                self.filename = None

    

class DummyIbobServer():
    def sendget_command(self, message):
        print "Dummy server: sending message:",message
        return '\r'
        
    def write_register(self, register, value):
        """
        given a register and a value, sends an iBOB command to set that register to
        that value, and checks for correct response on read
        """
        read = self.sendget_command("regwrite %s 0x%x" % (register, value))
        if read != "\r":
            print "Error: incorrect output read after sending command: regwrite %s 0x%x" \
                    % (register, value)
        
        # TODO: possibly add code to log this action in debug mode (have a column
        # that stores as strings the commands run)

    def read_register(self, register):
        """
        given a register, tries to read the value of the register
        """
        return self.sendget_control_message("regread %s" % register)
    
    def write_spec_info(self,spec_info_dict):
        print "dummy would add keys:",spec_info_dict