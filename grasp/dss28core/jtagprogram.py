"""
:mod:`dss28core.jtagprogram`
----------------------------

Utilities for interacting with the iBOB JTAG programmers through IMPACT

IMPACT batch mode has a bug/limitation where the JTAG clock rate cannot be speficified. This is
a problem because the iBOB JTAG chains require a clock rate of 1.5 MHz or less because of signal integrity issues.

To work around this, the bitfile must be incopreated into an Impact Project File (IPF) which has the clockrate set explicitly.

"""

import subprocess
import time,sys,re
import os
from tempfile import mkstemp
import config

cmd_template = """
loadprojectfile -file "%s"
setcable -p %s -baud 1500000
program -p 2 -prog
quit
"""

identify_cable_script = """
loadprojectfile -file "%s/ibobconn01_2009_Dec_12_1342.ipf"
setcable -p usb22 -baud 1500000
quit
""" % config.bitfiles_dir

def prog_ipf(ipf,cable='usb21', display = True):
    """
    Program an ipf using the indicated cable.
    """
    p,fname = proc_ipf(ipf,cable)
    tic = time.time()
    output = ''
    while p.returncode is None:
        print "polled",100*(time.time()-tic)/20.0,"% done"
        while True:
            out = p.stdout.read(1)
            if out == '' and p.poll() != None:
                break
            if out != '':
                output = ''.join((output,out))
                if display:
                    sys.stdout.write(out)
                    sys.stdout.flush()
            else:
                time.sleep(0.01)
        p.poll()
        time.sleep(1)
    tic = time.time()
#    print p.communicate()
#    print "communicate took",(time.time()-tic)*1000
    os.unlink(fname)
    return (p.returncode,output)
    
def proc_ipf(ipf,cable='usb21'):
    fname = mkstemp(suffix='.cmd', prefix='impact')[1]
    fh = open(fname,'w')
    fh.write(cmd_template % (ipf,cable))
    fh.close()
    p = subprocess.Popen('/home/gej/impact -batch ' + fname,bufsize=0,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    return (p,fname)

def identify_cables():
    """
    There are two JTAG programmers, one per bank of 4 iBOBs. Unfortunately, when the computer they are plugged into is rebooted, they are arbitrarily
    assigned to /dev/usb21 and /dev/usb22. The two programmers are slightly different models, so to distinguish them, we load a dummy project file
    and look at the cable information. The "type = " information seems to identify the cable type and is used to distinguish them.
    """
    fname = mkstemp(suffix='.cmd', prefix='impact')[1]
    fh = open(fname,'w')
    fh.write(identify_cable_script)
    fh.close()
    p = subprocess.Popen('/home/gej/impact -batch ' + fname,bufsize=0,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    #output = ''
    while p.returncode is None:
        p.poll()
        time.sleep(0.1)
    tic = time.time()
    output = p.communicate()
    print "communicate took",(time.time()-tic)*1000
    os.unlink(fname)
    if p.returncode == 0:
        result = output[0].lower()  #Avoid having to deal with case
        ports = re.findall('usb[0-9]+',result)  #Search for the strings usb21 and usb22
        cables = re.findall('type = 0x[0-9a-f]+',result)    #Search for the strings type = 0x0004 and type = 0x0005
        assert(len(ports)==2) #we only expect to search two ports currently
        assert(len(cables)==2) #and we only expect to find two cables
        cablemap = [None,None] # UpperBank, LowerBank
        if cables[0] == 'type = 0x0005':
            cablemap[0] = ports[0]
            cablemap[1] = ports[1]
        else:
            cablemap[0] = ports[1]
            cablemap[1] = ports[0]
    else:
        raise Exception('Calling impact failed with this msg:\n'+str(output[0]))
        
    return (cablemap,output)