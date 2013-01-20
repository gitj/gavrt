try:
    import tables
except:
    import dummytables as tables

import numpy as np
import time

from IbobPersonality import IbobPersonality


class TwoPolRealSpectrometer(IbobPersonality):
    """
    Personality for 2 input, 512 channel, total intensity spectrometer. 
    """
    def __init__(self,parent= None,adcClock=1024.0):
        self._adcClock = adcClock
        self._parent = parent
        if parent is not None:
            super(TwoPolRealSpectrometer,self).__init__(parent,adcClock=1024.0)       #Be sure to call base class init function
        

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
                    "II" : (512,),
                    "QQ" : (512,)
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
                    "adcI" : (8192,),
                    "adcQ" : (8192,)
                }
            }
        }
        self._measTypes = self._measTypesDict.keys()
    
        self._infoTable = {
            'ID':tables.UInt64Col(dflt=0),
            'Timestamp':tables.Float64Col(),
            'PeriodRegister':tables.UInt32Col(),
            'AccLenRegister':tables.UInt32Col(),
            'TvgRegister':tables.UInt32Col(),
            'IntegrationTime':tables.Float32Col(),
            'CommandHistory':tables.StringCol(128,dflt=' '), #Default value must be assigned to avoid pickling error Numpy bug #931
        }
        
    def restart(self):
        self.endUdp()
        self.adcReset(False)
        self.setIntegrationTime(4e-3)
        time.sleep(0.1)
        self.setIntegrationTime(40e-3)
        self.startUdp()
        
    def setIntegrationTime(self,t_int):
        """
        Set the integration time to args[0] seconds
        eg: setIntegrationTime(40e-3)
        t_int: integration time (seconds)
        
        """
        self._t_int = t_int
        acc_len = self._adcClock*1e6*t_int/(1024.0)       #Notice we need to know a property of the iBOB (ADC_clock) to calculate this
        period = acc_len*1024
        self.regwrite("cfgspec/vacc/acc_len",acc_len-1)
        self.regwrite("period",period-3)
        self._write_info({'PeriodRegister': (period-3),
                                 'AccLenRegister': (acc_len-1),
                                 'IntegrationTime': t_int})
        
    def setTvg(self,tvg):
        self.regwrite("tvg",tvg)

    def _bbfrq(self):
        return np.arange(512)*self._adcClock/1024.0
        
    def _reconstructMeasurement(self,m):
        if m.type == 'S':       #check what kind of data we got from the iBOB
            bram0 = m.brams[0,:].view(dtype='uint32').byteswap()        #reinterpret as uint32
            bram1 = m.brams[1,:].view(dtype='uint32').byteswap()
            # two inputs, each with 512 channels:
            dataI = np.empty((bram0.shape[0]/2,),'float')
            dataQ = np.empty((bram0.shape[0]/2,),'float')
            lsb_I_even = np.array(bram0[0::4],dtype='float')
            lsb_I_odd = np.array(bram0[1::4],dtype='float')
            lsb_Q_even = np.array(bram0[2::4],dtype='float')
            lsb_Q_odd = np.array(bram0[3::4],dtype='float')
            msb_I_even = np.array(bram1[0::4],dtype='float')
            msb_I_odd = np.array(bram1[1::4],dtype='float')
            msb_Q_even = np.array(bram1[2::4],dtype='float')
            msb_Q_odd = np.array(bram1[3::4],dtype='float')
            # if you take out this ".0" on the 2.0**32, it will break!
            msb_lsb_sum = (lsb_I_even + (msb_I_even *(2.0**32)))
            
            dataI[::2] = msb_lsb_sum 
            dataI[1::2] = lsb_I_odd + (msb_I_odd  *(2.0**32))
            dataQ[::2] = lsb_Q_even + (msb_Q_even *(2.0**32))
            dataQ[1::2] = lsb_Q_odd + (msb_Q_odd  *(2.0**32))
            
            #dataI and dataQ now have the properly interpreted data
            
            name = "SpectralPower"    #The name could be used to refer to the appropriate part of the h5 file hierarchy to store the data in
            arraydict = {"II": dataI,
                "QQ": dataQ}        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
                
            #the tabledict will be written to the table associated with the data array, one key per column.
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
            #               "Mean":m.extra_param_20}   #notice, the generic ExtraParam20 now gets renamed to something meaningful
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            return (name, arraydict, tabledict)
        if m.type == 'A':
            adcI = np.array(m.brams[0,:].view(dtype='int8'),dtype='float')
            adcQ = np.array(m.brams[1,:].view(dtype='int8'),dtype='float')
            
            name = "ADCSnapshot"
            arraydict = {"adcI": adcI,
                "adcQ": adcQ}        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
#                           "Mean":m.extra_param_20}   #notice, the generic ExtraParam20 now gets renamed to something meaningful
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            
        return (name, arraydict, tabledict)
