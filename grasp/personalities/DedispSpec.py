try:
    import tables
except:
    import dummytables as tables

import numpy as np
import time

from IbobPersonality import IbobPersonality


class _BaseDedisp(IbobPersonality):
    def __init__(self,parent = None,adcClock=1024.0):
        self._adcClock = adcClock
        self._parent = parent
        super(_BaseDedisp,self).__init__(parent=parent,adcClock=adcClock)       #Be sure to call base class init function
                
        self._measTypesDict = {
            "SpectralPower" : {
                "table" : {
                    'ID':tables.Int64Col(),
                    'AccNumber':tables.Int64Col(),
                    'Timestamp':tables.Float64Col(),
                    'LoadIndicator':tables.Int32Col(),
                    'MasterCounter':tables.UInt32Col()
                },
                "arrays": {
                    "II" : (256,)
                },
            },

            "ADCSnapshot" : {
                "table" : {
                    'ID':tables.Int64Col(),
                    'AccNumber':tables.Int64Col(),
                    'Timestamp':tables.Float64Col(),
                    'LoadIndicator':tables.Int32Col(),
                    'MasterCounter':tables.UInt32Col()
                    
                },
                "arrays": {
                    "adcI" : (16384,)
                }
            },
            "DedispersedTotalPower" : {
               "table" : {
                  'ID':tables.Int64Col(),
                  'AccNumber':tables.Int64Col(),
                  'Timestamp':tables.Float64Col(),
                  'Threshold':tables.Float32Col(),
                  'MasterCounter':tables.UInt32Col()
                  },
                  "arrays": {
                     "II" : (2048,)
                 }
          },
            "TriggeredDedispersedTotalPower" : {
               "table" : {
                  'ID':tables.Int64Col(),
                  'AccNumber':tables.Int64Col(),
                  'Timestamp':tables.Float64Col(),
                  'MasterCounter':tables.UInt32Col()
                  },
                  "arrays": {
                     "II" : (2048,)
                 }
          }
        }
        self._measTypes = self._measTypesDict.keys()
    
    
        self._controlRegisters = {'arm':0,
                                   "tvg":0,
                                   'coeff':0,
                                   'cs/IDD/thresh':0,
                                   'cs/IDD/iddctrl':0,
                                   'cs/IDD/postdly0':0,
                                   'cs/vacc/acc_len':0,
                                   'ctrl':0,
                                   'iddaddr':0,
                                   'period':0,
                                   'period1':0}    
    
        self._infoTable = {
            'ID':tables.UInt64Col(dflt=0),
            'Timestamp':tables.Float64Col(),
            'IntegrationTime':tables.Float32Col(),
            'CommandHistory':tables.StringCol(128,dflt=' '), #Default value must be assigned to avoid pickling error Numpy bug #931
        }
        
        for r in self._controlRegisters:
            self._infoTable[r.replace('/','_')] = tables.UInt32Col()
        
        if self._parent:
            self.read_from_iBOB()
        
        
    def startUdp(self,ip='192.168.0.2',mode=0x1C00168):
        IbobPersonality.startUdp(self, ip=ip, mode=mode)

    def restart(self):
        self.endUdp()
        self.adcReset(True)
        self.setIntegrationTime(4e-3)
        time.sleep(0.1)
        self.setIntegrationTime(40e-3)
        self.setTvg(0x440000)
        self.startUdp()
    def setIntegrationTime(self,t_int):
        """
        Set the integration time to args[0] seconds
        eg: setIntegrationTime(40e-3)
        t_int: integration time (seconds)
        ADC_clock: MHz
        
        """
        
        acc_len = self._adcClock*1e6*t_int/(1024.0)      
        if acc_len > 65536:
            raise("Integration time is too long:",t_int)
        self._t_int = t_int
        
        #acc_len = 2048 # hardwire for now to known working condition
        period = acc_len*16384
        
        self.regwrite("cs/vacc/acc_len",acc_len-1)
        self.regwrite("period1",period-2)
        self._write_info({'IntegrationTime': t_int})
        
    def setTvg(self,tvg):
        self.regwrite("tvg",tvg)
        
        
    def bumpEQ(self,factor):
        self.setEQ(self.eqI*factor)
        
    def zapEQ(self,mask):
        eqs = self.eqI.copy()
        eqs[mask] = 0
        self.setEQ(eqs)
        
    def setEQ(self,eqI):
        self.eqI = eqI
        eqI = (eqI*8).round().astype('int')
        
        t = time.time()
        tries = 0
        for k in range(eqI.shape[0]):
            #tries += self.setcheck('iddaddr',k)
            data = 8<<28
            data += eqI[k]
            #tries += self.setcheck('coeff',data)
            tries += self.sendcoeff(k, data)
            #print k,":",tries
        self.regwrite('coeff',0)
        print "done in",(time.time()-t), "tries",tries
        
    def setIDD(self,coeffs,smooth=1,reverse=False):
        tries = 0
        for k in range(coeffs.shape[0]):
            for m in range(coeffs.shape[1]):
                data = 1<<29    #iddwe is bit 29
                addr = (k<<10) + (m)
                data += coeffs[k,m]
                tries += self.sendcoeff(addr,data)
        self.regwrite('coeff',0)
        ctrl = self._controlRegisters['cs/IDD/iddctrl'] & 0x3 #save trigger settings in bottom two bits
        if reverse:
            ctrl += 0x4
        ctrl += ((smooth & 0x3)<<4)
        self.regwrite('cs/IDD/iddctrl',ctrl)
        
    def setPostdly(self,val):
        self.regwrite('cs/IDD/postdly0',val)
    
    def setThreshold(self,val):
        self.regwrite('cs/IDD/thresh',val)
        
    def sendcoeff(self,addr,coeff):
        tries = 0
        while True:
            res = self._sendget('setcoeff 0x%08X 0x%08X' % (addr,coeff))
            try:
                res = res.find('OK')
            except:
                pass
            #print "res:",res,"data:",data
            tries += 1
            if res > -1:
                return tries
            
            
        
    def _bbfrq(self):
        raise Exception("Not implemented")

    def _reconstructMeasurement(self,m):
        if m.type == 'S':       #check what kind of data we got from the iBOB
            bram0 = m.brams[0,:].view(dtype='uint32').byteswap()        #reinterpret as uint32
            dataI = bram0.astype('float')
            #dataI and dataQ now have the properly interpreted data
            
            name = "SpectralPower"    #The name could be used to refer to the appropriate part of the h5 file hierarchy to store the data in
            arraydict = {"II": dataI
                }        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.

            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
            #               "Mean":m.extra_param_20}   #notice, the generic ExtraParam20 now gets renamed to something meaningful
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            return (name, arraydict, tabledict)
        if m.type == 'A':
            bram0 = m.brams[0,:].view(dtype='int8').astype('float32')
            bram1 = m.brams[1,:].view(dtype='int8').astype('float32')
            adcI = np.zeros((bram0.shape[0]*2,),dtype='float32')
            
            adcI[0::8] = bram1[3::4]
            adcI[1::8] = bram1[2::4]
            adcI[2::8] = bram1[1::4]
            adcI[3::8] = bram1[0::4]
            adcI[4::8] = bram0[3::4]
            adcI[5::8] = bram0[2::4]
            adcI[6::8] = bram0[1::4]
            adcI[7::8] = bram0[0::4]
            #adcQ = np.array(m.brams[1,:].view(dtype='int8'),dtype='float')
            
            name = "ADCSnapshot"
            arraydict = {"adcI": adcI}        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
#                           "Mean":m.extra_param_20}   #notice, the generic ExtraParam20 now gets renamed to something meaningful
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            return (name, arraydict, tabledict)
        if m.type == 'E':
            data = np.array(m.brams[0,:].view(dtype='uint32').byteswap(),dtype='float')
            offset = m.extra_param_18
            data = np.roll(data,-offset,axis=0)
            name = "TriggeredDedispersedTotalPower"
            tabledict = {
                  'AccNumber':m.accum_num,
                  'Timestamp':m.timestamp,
                  "MasterCounter":m.master_counter
                  }
            arraydict = {
                     "II" : data
                 }
            return (name, arraydict, tabledict)
        if m.type == 'D':
            data = np.array(m.brams[0,:].view(dtype='uint32').byteswap(),dtype='float')
            name = "DedispersedTotalPower"
            tabledict = {
                  'AccNumber':m.accum_num,
                  'Timestamp':m.timestamp,
                  "MasterCounter":m.master_counter,
                  'Threshold':m.extra_param_20,
                  }
            arraydict = {
                     "II" : data
                 }
            return (name, arraydict, tabledict)
        print "uh oh, got unknown measurement type:",m.type
            
class DDCDedisp(_BaseDedisp):
    def __init__(self,parent = None,adcClock=1024.0):
        print "ddcdedisp: got adcClock",adcClock
        super(DDCDedisp,self).__init__(parent=parent,adcClock=1024.0)       #Be sure to call base class init function
        
        self._adcClock = adcClock # APparently I don't understand class attributes, because this is not getting set properly in the super call
        print "ddcdedisp: now adcClock",self._adcClock
    def _bbfrq(self):
        return np.fft.fftshift(np.arange(384,640))*self._adcClock/1024.0
    
    def _iddfrq(self):
        return np.arange(384,640)*self._adcClock/1024.0
    
class WideX4Dedisp(_BaseDedisp):
    def __init__(self,parent = None,adcClock=1024.0):
        super(WideX4Dedisp,self).__init__(parent,adcClock=1024.0)       #Be sure to call base class init function
        self._adcClock = adcClock

    def _bbfrq(self):
        ch0 = (self._controlRegisters['tvg'] & 0x30)>>4
        return np.arange(ch0,1024,4)*self._adcClock/1024.0
    
    def _iddfrq(self):
        return self._bbfrq()
