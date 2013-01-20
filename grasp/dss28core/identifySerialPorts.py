"""
:mod:`dss28core.identifySerialPorts`
------------------------------------

Identify which devices are connected to which serial port on the main SPSS computer (typically cumin).

This program should be run once at bootup because the serial ports get arbitrarily assigned to /dev/ttyUSB* nodes.

The results are written as a pickled dictinary to /home/dss28/serialPorts.pkl
"""
import serial
import logging
import struct
import os
import glob
import cPickle
from loggers import corelog as logger
DSS28_DIR = '/home/dss28' 


def identify():
    """
    Identify all serial ports.
    
    The TCT should be identified first since it uses non-standard 7-O-1 encoding

    Returns a dictionary mapping device name to serial port.
    
    We don't currently identify the ROACH, IBOB_A, IBOB_B, and BEE2 serial ports because they are rarely used an easy to identify after
    the other ports have been discovered.
    """
    
    # The following maps device name to the function used to find it.
    devFuncMap = [('TCT',findTCT),
              ('Power',findPowerSupply),
              ('Valon',findValon)]
    ports = glob.glob('/dev/ttyUSB*')  # Get a list of all serial ports
    ports.sort()
    portmap = {}
    for Name,findFunc in devFuncMap:
        port = findFunc(ports)
        portmap[Name] = port
        ports.remove(port)
        
    return portmap

def store(portmap):
    """
    Pickle and store the portmap. The filename is currently fixed.
    """
    fname = os.path.join(DSS28_DIR,'serialPorts.pkl')
    pkl = open(fname,'w')
    cPickle.dump(portmap,pkl)
    pkl.close()
    os.chmod(fname, 0777)
def findTCT(devs):
    """
    Identify the TCT by looking for the time stamp emitted at 1 second intervals
    """
    for dev in devs:
        try:
            ser = serial.Serial(dev)
        except Exception,e:
            logger.warning("findTCT: Couldn't open serial port: %s... moving to next port" % dev)
            continue
        ser.setBaudrate(9600)
        ser.setParity('O')
        ser.setByteSize(7)
        ser.setTimeout(2)            
        ser.flushInput()
        tries = 0
        while tries < 2:
            r = ser.readline(eol='\r')
            logger.debug('findTCT: Trying %s Trial # %d got %s' % (dev,tries,r))
            if len(r) == 18:
                break
            tries += 1
        if tries == 2:
            logger.info('findTCT: failed to find TCT on port %s' % dev)
            ser.setParity('N')
            ser.setByteSize(8)
            ser.close()
            continue
        else:
            logger.info('findTCT: Found TCT on port %s' % dev)
            ser.close()
            return dev
        
        
def findPowerSupply(devs):
    """
    Identify the Lambda Power supply which provdes +5 V to the SPSS equipment 
    """
    for dev in devs:
        try:
            ser = serial.Serial(dev)
        except Exception,e:
            logger.warning("findPowerSupply: Couldn't open serial port: %s... moving to next port" % dev)
            continue
        ser.setBaudrate(9600)
        ser.setParity('N')
        ser.setByteSize(8)
        ser.setTimeout(1)            
        ser.flushInput()
        tries = 0
        while tries < 3:
            ser.flushInput()
            ser.write('adr 6\r')
            r = ser.readline(eol='\r')

            logger.debug('findPowerSupply: Trying %s Trial # %d got %s' % (dev,tries,r))
            if r.find('OK') >= 0:
                break
            tries += 1
        if tries == 3:
            logger.info('findPowerSupply: failed to find on port %s' % dev)
            ser.close()
            continue
        else:
            logger.info('findPowerSupply: Found on port %s' % dev)
            ser.close()
            return dev


def findValon(devs):
    """
    Identify the Valon synthesizer
    """
    for dev in devs:
        try:
            ser = serial.Serial(dev)
        except Exception,e:
            logger.warning("findValon: Couldn't open serial port: %s... moving to next port" % dev)
            continue
        ser.setBaudrate(9600)
        ser.setParity('N')
        ser.setByteSize(8)
        ser.setTimeout(1)            
        ser.flushInput()
        tries = 0
        while tries < 3:
            ser.flushInput()
            ser.write(struct.pack('>B',0x82))
            r = ser.read(16)
            logger.debug('findValon: Trying %s Trial # %d got %s' % (dev,tries,r))
            if r.find('Synth') >= 0:
                break

            tries += 1
        if tries == 3:
            logger.info('findValon: failed to find on port %s' % dev)
            ser.close()
            continue
        else:
            logger.info('findValon: Found on port %s' % dev)
            ser.close()
            return dev
        
if __name__ == '__main__':
    pm = identify()
    store(pm)
    print pm
