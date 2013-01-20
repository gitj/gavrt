try:
    import tables
except:
    import dummytables as tables

import numpy as np
import time

from IbobPersonality import IbobPersonality

class TwoPolDDCSpectrometer(IbobPersonality):
    """
    Personality for 2 input, 8192 channel, total intensity spectrometer. 
    """
    def __init__(self,parent= None,adcClock=256.0):
        self._adcClock = adcClock
        self._parent = parent
        #if parent is not None:
        super(TwoPolDDCSpectrometer,self).__init__(parent,adcClock=adcClock)       #Be sure to call base class init function
        
        self._t_int = 256e-3
        self._measTypesDict = {
            "SpectralPower" : {
                "table" : {
                    'ID':tables.Int64Col(),
                    'AccNumber':tables.Int64Col(),
                    'Timestamp':tables.Float64Col(),
                    'LoadIndicator':tables.Int32Col(),
                    'MasterCounter':tables.UInt32Col(),
                    'SyncTime':tables.UInt32Col(),
                    'IntegrationTime':tables.Float64Col()
                },
                "arrays": {
                    "II" : (8192,),
                    "QQ" : (8192,)
                },
            },

            "ADCSnapshot" : {
                "table" : {
                    'ID':tables.Int64Col(),
                    'AccNumber':tables.Int64Col(),
                    'Timestamp':tables.Float64Col(),
                    'LoadIndicator':tables.Int32Col(),
                    'MasterCounter':tables.UInt32Col(),
                    'SyncTime':tables.UInt32Col()
                    
                },
                "arrays": {
                    "adcI" : (8192,),
                    "adcQ" : (8192,)
                }
            }
        }
        self._measTypes = self._measTypesDict.keys()
        
        self._controlRegisters = {'ctrl':0,
                                   'cfgspec/vacc/acc_len':0,
                                   'period':0,
                                   }
        self._infoTable = {
            'ID':tables.UInt64Col(dflt=0),
            'Timestamp':tables.Float64Col(),
            'PeriodRegister':tables.UInt32Col(),
            'AccLenRegister':tables.UInt32Col(),
            'TvgRegister':tables.UInt32Col(),
            'IntegrationTime':tables.Float32Col(),
            'CommandHistory':tables.StringCol(128,dflt=' '), #Default value must be assigned to avoid pickling error Numpy bug #931
        }
        for r in self._controlRegisters: 
            self._infoTable[r.replace('/','_')] = tables.UInt32Col()
                
    def restart(self):
        self.endUdp()
        self.adcReset(False)
        self.setIntegrationTime(8e-3)
        time.sleep(0.1)
        self.setIntegrationTime(256e-3)
        self.startUdp()
        
    def setIntegrationTime(self,t_int):
        """
        Set the integration time in seconds
        eg: setIntegrationTime(40e-3)
        t_int: integration time (seconds)
        
        """
        acc_len = np.round((self._adcClock/4)*1e6*t_int/(8192.0))
        period = acc_len*8192
        if acc_len >= 2**16-1:
            raise Exception("Requested integration length results in acc_len setting %d > 65535. Reduce requested integration time." % (acc_len-1,))
        t_int_actual = acc_len*8192/((self._adcClock/4)*1e6)
        self._t_int = t_int_actual
        self.regwrite("cfgspec/vacc/acc_len",acc_len-1)
        self.regwrite("period",period-2)
        self._write_info({'PeriodRegister': (period-2),
                                 'AccLenRegister': (acc_len-1),
                                 'IntegrationTime': t_int_actual})
        self.sync()

    def sync(self):
        while np.abs(np.fmod(time.time(),1)-0.25)> 0.1:
            pass
        while np.abs(np.fmod(time.time(),1)-0.25)< 0.1:
            pass
        stt = int(np.ceil(time.time()))
        self._sendget('pps x%x' % stt)
        print "armed at:",stt
        
    def setTvg(self,tvg):
        self.regwrite("tvg",tvg)

    def _bbfrq(self):
        return self._adcClock*(1 + 1/8.0) + np.arange(8192)*(self._adcClock/4.0)/8192.0   
    #Assuming 270-370 filter, sampling clock of 256 MHz 3rd nyquist zone, and assumes data has been FFTSHIFTED
    # In first nyquist, sampled band is 0-128 MHz. Half band filter returns 32 (=256/8) to 96 MHz (BW=64=256/4)
    # 3rd zone is 256 + 256/8. 8k channels across 256/4 = 64 MHz
        
    def _reconstructMeasurement(self,m):
        if m.type == 'S':       #check what kind of data we got from the iBOB
            bram0 = m.brams[0,:].view(dtype='uint32').byteswap()        #reinterpret as uint32
            bram1 = m.brams[1,:].view(dtype='uint32').byteswap()
            # two inputs, each with 512 channels:
            dataI = np.fft.fftshift(bram1.astype('float32'))
            dataQ = np.fft.fftshift(bram0.astype('float32'))
            
            name = "SpectralPower"    #The name could be used to refer to the appropriate part of the h5 file hierarchy to store the data in
            arraydict = {"II": dataI,
                "QQ": dataQ}        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
                
            #the tabledict will be written to the table associated with the data array, one key per column.
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter,
                           "SyncTime":m.extra_param_20,
                           "IntegrationTime":self._t_int}
                           
            return (name, arraydict, tabledict)
        if m.type == 'A':
            adcI = np.array(m.brams[1,:].view(dtype='int8'),dtype='float')
            adcQ = np.array(m.brams[0,:].view(dtype='int8'),dtype='float')
            
            name = "ADCSnapshot"
            arraydict = {"adcI": adcI,
                "adcQ": adcQ}        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter,
                           "SyncTime":m.extra_param_20}
            
        return (name, arraydict, tabledict)
