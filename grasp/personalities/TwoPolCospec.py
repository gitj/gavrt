try:
    import tables
except:
    import dummytables as tables

import numpy as np
import time

from IbobPersonality import IbobPersonality


class Cospec(IbobPersonality):
    def __init__(self,parent = None,adcClock=1024.0):
        self._adcClock = adcClock
        self._parent = parent
        if parent is not None:
            super(Cospec,self).__init__(parent,adcClock=adcClock)       #Be sure to call base class init function
        
        self._measTypesDict = {
            "ADCSnapshot" : {
                "table" : {
                    'ID':tables.Int64Col(),
                    'Timestamp':tables.Float64Col(),
                    'LoadIndicator':tables.Int32Col(),
                    'MasterCounter':tables.UInt32Col()
                    
                },
                "arrays": {
                    "adcI" : (16384,)
                }
            }
        }
        self._measTypes = self._measTypesDict.keys()
    
        self._controlRegisters = {'select':0,
                                   'fftshift':0,
                                   'period':0,
                                   }
    
        self._infoTable = {
            'ID':tables.UInt64Col(dflt=0),
            'Timestamp':tables.Float64Col(),
            'CommandHistory':tables.StringCol(128,dflt=' '), #Default value must be assigned to avoid pickling error Numpy bug #931
        }
        
        for r in self._controlRegisters:
            self._infoTable[r.replace('/','_')] = tables.UInt32Col()
        
        if self._parent:
            self.readFromIbob()
            
        self._nfft = 32
        self._nchperclk = 2
                        
    def adcReset(self):
        """
        Reset the ADC in interleaved mode
        """
        return IbobPersonality.adcReset(True)
    
    def restart(self):
        self.adcReset(True)
        
    def setIntegrationTime(self,t_int):
        """
        Set the integration time to args[0] seconds
        eg: setIntegrationTime(40e-3)
        t_int: integration time (seconds)
        ADC_clock: MHz
        
        """
        pass
    
    def getBBChans(self):
        nfft = self._nfft
        nchperclk = self._nchperclk
        select = self._controlRegisters['select']
        fftshift = self._controlRegisters['fftshift']
        if fftshift & 0x04000000:
            xpose = False
        else:
            xpose = True
        
        msk = select & 0xFFFF
        sel = (select>>16) & 0xFFFF
        nclks = nfft/nchperclk
        chans = np.arange(nfft).reshape((nclks,nchperclk))
        out = []
        for k in range(0,nclks,nchperclk):
            sub = chans[k:(k+nchperclk),:]
            if xpose:
                sub = sub.T
            for m in range(nchperclk):
                thisclk = k+m
                if (thisclk & msk) == sel:
                    for ch in sub[m,:]:
                        out.append(ch)
        return np.array(out)
    
    def _bbfrq(self):
        return self.getBBChans()*self._adcClock/(2*self._nfft)

    def _reconstructMeasurement(self,m):
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
            
            
            
        return (name, arraydict, tabledict)
