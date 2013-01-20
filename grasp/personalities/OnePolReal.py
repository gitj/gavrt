
try:
    import tables
except:
    import dummytables as tables

import numpy as np
import time

from IbobPersonality import IbobPersonality

class OnePolRealSpectrometer(IbobPersonality):
    def __init__(self,parent = None,adcClock=1024.0):
        self._adcClock = adcClock
        self._parent = parent
        super(OnePolRealSpectrometer,self).__init__(parent,adcClock=adcClock)       #Be sure to call base class init function
        
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
                    "II" : (1024,)
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
        
        self._controlRegisters = {'ctrl':0,
                                   'cfgspec/vacc/acc_len':0,
                                   'period':0,
                                   }
        
        for r in self._controlRegisters: 
            self._infoTable[r.replace('/','_')] = tables.UInt32Col()
    
    def adcReset(self,interleaved=True):
        """
        Reset the ADC
        """
        return IbobPersonality.adcReset(self,interleaved)
            
    def restart(self):
        """ 
        Start/Restart the spectrometer.
        """
        self.endUdp()
        self.adcReset(True)
        self.setIntegrationTime(4e-3)
        time.sleep(0.1)
        self.setIntegrationTime(40e-3)
        self.start_udp()
    def setIntegrationTime(self,t_int):
        """
        Set the integration time to args[0] seconds
        eg: setIntegrationTime(40e-3)
        t_int: integration time (seconds)
        ADC_clock: MHz
        
        """
        self._t_int = t_int
        acc_len = self._adcClock*1e6*t_int/(1024.0)       #Notice we need to know a property of the iBOB (ADC_clock) to calculate this
        period = acc_len*1024
        self.regwrite("cfgspec/vacc/acc_len",acc_len-1)
        self.regwrite("period",period-2)
        self._write_info({'IntegrationTime': t_int})
        
    def setTvg(self,tvg):
        self.regwrite("tvg",tvg)
        
    def _bbfrq(self):
        return np.arange(1024)*self._adcClock/1024.0

    def _reconstructMeasurement(self,m):
        if m.type == 'S':       #check what kind of data we got from the iBOB
            bram0 = m.brams[0,:].view(dtype='uint32').byteswap()        #reinterpret as uint32
            bram1 = m.brams[1,:].view(dtype='uint32').byteswap()
            dataI = np.empty((bram0.shape[0],),'float')

            msb_lsb_sum = (bram0 + (bram1 *(2.0**32)))
            
            dataI[:] = msb_lsb_sum 
            
            #dataI and dataQ now have the properly interpreted data
            
            name = "SpectralPower"    #The name could be used to refer to the appropriate part of the h5 file hierarchy to store the data in
            arraydict = {"II": dataI
                }        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
#            if self._firstts == 0:
#                self._firstts = m.timestamp
#                self._lastacc = np.array([m.accum_num],dtype='uint16')
#            else:
#                timestep = (np.array([m.accum_num],dtype='uint16')-self._lastacc).astype('int64')
#                self._fullacc += timestep
#                self._lastacc = np.array([m.accum_num],dtype='uint16')
#                #print timestep*self._t_int,self._t_int,time.ctime(m.timestamp),time.ctime(self._firstts)
#                m.timestamp = self._firstts + self._fullacc*self._t_int
#                #print time.ctime(m.timestamp)
#                
#            
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


class OnePolReal512ChannelSpectrometer(OnePolRealSpectrometer):
    def __init__(self,parent = None,adcClock=1024.0):
        self._adcClock = adcClock
        
        self._parent = parent
        super(OnePolReal512ChannelSpectrometer,self).__init__(parent,adcClock=adcClock)       #Be sure to call base class init function

        self._measTypesDict["SpectralPower"]["arrays"] = {
                    "II" : (512,),
                }
        
        self._controlRegisters = {'ctrl':0,
                                   'cs/vacc/acc_len':0,
                                   'period':0,
                                   }
        
        for r in self._controlRegisters:
            self._infoTable[r.replace('/','_')] = tables.UInt32Col()

    def restart(self):
        self.endUdp()
        self.adcReset(True)
        self.setIntegrationTime(4e-3)
        time.sleep(0.1)
        self.setIntegrationTime(20e-3)
        self.startUdp()
        
    def setIntegrationTime(self,t_int):
            """
            Set the integration time to args[0] seconds
            eg: setIntegrationTime(40e-3)
            t_int: integration time (seconds)
            
            """
            acc_len = self._adcClock*1e6*t_int/(512.0)     
            if acc_len > 65536:
                raise("Integration time is too long:",t_int)
            self._t_int = t_int
            period = acc_len*1024
            self.regwrite("cs/vacc/acc_len",acc_len-1)
            self.regwrite("period",period-2)
            self._write_info({'IntegrationTime': t_int})
            
            
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
            data = 0x80000000
            data += eqI[k]
            data += (k<<20)
            tries += self.sendcoeff(data)
        self.regwrite('cs/coeff',0)
        print "done in",(time.time()-t), "tries",tries
        
    def sendcoeff(self,data):
        self._write_register('cs/coeff', data)
        return 1

    def _bbfrq(self):
        return np.arange(512)*self._adcClock/512.0

    def _reconstructMeasurement(self,m):
        if m.type == 'S':       #check what kind of data we got from the iBOB
            bram0 = m.brams[0,:].view(dtype='uint32').byteswap()[:512]        #reinterpret as uint32
            bram1 = m.brams[1,:].view(dtype='uint32').byteswap()[:512]
            dataI = np.empty((bram0.shape[0],),'float')

            msb_lsb_sum = (bram0 + (bram1 *(2.0**32)))
            
            dataI[:] = msb_lsb_sum 
                        
            
            name = "SpectralPower"    #The name could be used to refer to the appropriate part of the h5 file hierarchy to store the data in
            arraydict = {"II": dataI,
                }        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            return (name, arraydict, tabledict)
        if m.type == 'B':
            data = m.brams[0,:].view(dtype='uint8')[:512]
            dataI = data.astype('float')
            name = "SpectralPower"
            arraydict = {"II": dataI}
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
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
            
            name = "ADCSnapshot"
            arraydict = {"adcI": adcI}        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
            
        return (name, arraydict, tabledict)

class OnePolRealKurtosisSpectrometer(IbobPersonality):
    def __init__(self,parent = None,adcClock=1024.0):
        self._adcClock = adcClock
        
        self._parent = parent
        self._mode = 0
        
        self._t_int = 40e-3 # TODO: Should read and compute the real value
        
        if parent is not None:
            super(OnePolRealKurtosisSpectrometer,self).__init__(parent,adcClock=adcClock)       #Be sure to call base class init function
        
        self._measTypesDict = {
            "SpectralPower" : {
                "table" : {
                    'ID':tables.Int64Col(),
                    'AccNumber':tables.Int64Col(),
                    'Timestamp':tables.Float64Col(),
                    'LoadIndicator':tables.Int32Col(),
                    'MasterCounter':tables.UInt32Col(),
                    'SyncTime':tables.UInt32Col(),
                    'IntegrationTime':tables.Float32Col()
                },
                "arrays": {
                    "II" : (1024,),
                    "SK" : (1024,),
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
                    "adcI" : (16384,)
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
    
        
    def requestAdcSnapshot(self):
        """
        Request an ADC snapshot
        args: none
        """
        return self._regwrite('snap/ctrl',7)
    
    def restart(self):
        self.endUdp()
        self.adcReset(True)
        self.setIntegrationTime(4e-3)
        time.sleep(0.1)
        self.setIntegrationTime(40e-3)
        self.startUdp()
        self.sync()
    
    def sync(self):
        while np.abs(np.fmod(time.time(),1)-0.25)> 0.1:
            pass
        stt = int(np.ceil(time.time()))
        self._sendget('pps x%x' % stt)
        print "armed at:",stt
                    

    def setIntegrationTime(self,t_int):
        """
        Set the integration time to args[0] seconds
        eg: setIntegrationTime(40e-3)
        t_int: integration time (seconds)
        ADC_clock: MHz
        
        """
        self._t_int = t_int
        acc_len = self._adcClock*1e6*t_int/(1024.0)       #Notice we need to know a property of the iBOB (ADC_clock) to calculate this
        period = acc_len*1024
        self.regwrite("cs/vacc/acc_len",acc_len-1)
        self.regwrite("period",period-2)
        self._write_info({'IntegrationTime': t_int})
        
    def setTvg(self,tvg):
        self.regwrite("tvg",tvg)
        
    def _bbfrq(self):
        return np.arange(1024)*self._adcClock/1024.0

    def _reconstructMeasurement(self,m):
        if m.type == 'S':       #check what kind of data we got from the iBOB
            bram0 = m.brams[0,:].view(dtype='uint32').byteswap()        #reinterpret as uint32
            bram1 = m.brams[1,:].view(dtype='uint32').byteswap()
            dataI = np.empty((bram0.shape[0],),'float')

            msb_lsb_sum = (bram0 + (bram1 *(2.0**32)))
            
            dataI[:] = msb_lsb_sum 
            
            bram2 = m.brams[2,:].view(dtype='uint32').byteswap().astype('float')        #reinterpret as uint32
            bram3 = m.brams[3,:].view(dtype='uint32').byteswap().astype('float')
            
            skdata = (bram2 + (bram3 *(2.0**32)))
            
            #dataI and dataQ now have the properly interpreted data
            
            name = "SpectralPower"    #The name could be used to refer to the appropriate part of the h5 file hierarchy to store the data in
            arraydict = {"II": dataI,
                         "SK": skdata
                }        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
#            if self._firstts == 0:
#                self._firstts = m.timestamp
#                self._lastacc = np.array([m.accum_num],dtype='uint16')
#            else:
#                timestep = (np.array([m.accum_num],dtype='uint16')-self._lastacc).astype('int64')
#                self._fullacc += timestep
#                self._lastacc = np.array([m.accum_num],dtype='uint16')
#                #print timestep*self._t_int,self._t_int,time.ctime(m.timestamp),time.ctime(self._firstts)
#                m.timestamp = self._firstts + self._fullacc*self._t_int
#                #print time.ctime(m.timestamp)
#                
#            
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter,
                           "SyncTime":m.extra_param_20,
                           "IntegrationTime":self._t_int}
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            return (name, arraydict, tabledict)
        if m.type == 'B':
            data = m.brams[0,:].view(dtype='uint8')
            dataI = data.astype('float')
#            scale = 1<<(self._mode & 0x07)
#            dataI *= scale
            name = "SpectralPower"
            arraydict = {"II": dataI}
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter,    
                           "SyncTime":m.extra_param_20}
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
                           "MasterCounter":m.master_counter,
                           "SyncTime":m.extra_param_20}
#                           "Mean":m.extra_param_20}   #notice, the generic ExtraParam20 now gets renamed to something meaningful
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            
        return (name, arraydict, tabledict)

class OnePolReal512ChannelKurtosisSpectrometer(OnePolRealKurtosisSpectrometer):
    def __init__(self,parent = None,adcClock=1024.0):
        self._adcClock = adcClock
        
        self._parent = parent
        super(OnePolReal512ChannelKurtosisSpectrometer,self).__init__(parent,adcClock=adcClock)       #Be sure to call base class init function

        self._measTypesDict["SpectralPower"]["arrays"] = {
                    "II" : (512,),
                    "SK" : (512,),
                }

    def restart(self):
        self.endUdp()
        self.adcReset(True)
        self.setIntegrationTime(4e-3)
        time.sleep(0.1)
        self.setIntegrationTime(20e-3)
        self.startUdp()
        
    def setIntegrationTime(self,t_int):
            """
            Set the integration time to args[0] seconds
            eg: setIntegrationTime(40e-3)
            t_int: integration time (seconds)
            
            """
            acc_len = self._adcClock*1e6*t_int/(512.0)     
            if acc_len > 65536:
                raise("Integration time is too long:",t_int)
            self._t_int = t_int
            period = acc_len*1024
            self.regwrite("cs/vacc/acc_len",acc_len-1)
            self.regwrite("period",period-2)
            self._write_info({'IntegrationTime': t_int})
            
    def _bbfrq(self):
        return np.arange(512)*self._adcClock/512.0

    def _reconstructMeasurement(self,m):
        if m.type == 'S':       #check what kind of data we got from the iBOB
            bram0 = m.brams[0,:].view(dtype='uint32').byteswap()[:512]        #reinterpret as uint32
            bram1 = m.brams[1,:].view(dtype='uint32').byteswap()[:512]
            dataI = np.empty((bram0.shape[0],),'float')

            msb_lsb_sum = (bram0 + (bram1 *(2.0**32)))
            
            dataI[:] = msb_lsb_sum 
            
            bram2 = m.brams[2,:].view(dtype='uint32').byteswap().astype('float')        #reinterpret as uint32
            bram3 = m.brams[3,:].view(dtype='uint32').byteswap().astype('float')
            
            skdata = (bram2 + (bram3 *(2.0**32)))
            
            #dataI and dataQ now have the properly interpreted data
            
            name = "SpectralPower"    #The name could be used to refer to the appropriate part of the h5 file hierarchy to store the data in
            arraydict = {"II": dataI,
                         "SK": skdata
                }        #This is a dictionary of the data to be put in the measurement arrays. The keys provide the names of the arrays
            
            #the tabledict will be written to the table associated with the data array, one key per column.
#            if self._firstts == 0:
#                self._firstts = m.timestamp
#                self._lastacc = np.array([m.accum_num],dtype='uint16')
#            else:
#                timestep = (np.array([m.accum_num],dtype='uint16')-self._lastacc).astype('int64')
#                self._fullacc += timestep
#                self._lastacc = np.array([m.accum_num],dtype='uint16')
#                #print timestep*self._t_int,self._t_int,time.ctime(m.timestamp),time.ctime(self._firstts)
#                m.timestamp = self._firstts + self._fullacc*self._t_int
#                #print time.ctime(m.timestamp)
#                
#            
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
            #               "Mean":m.extra_param_20}   #notice, the generic ExtraParam20 now gets renamed to something meaningful
            
            #no need to return anything, m.tabledict and m.arraydict can be passed on to the dataserver now
            return (name, arraydict, tabledict)
        if m.type == 'B':
            data = m.brams[0,:].view(dtype='uint8')[:512]
            dataI = data.astype('float')
#            scale = 1<<(self._mode & 0x07)
#            dataI *= scale
            name = "SpectralPower"
            arraydict = {"II": dataI}
            tabledict = {"Timestamp":m.timestamp,
                           "AccNumber":m.accum_num,
                           "LoadIndicator":m.load_indicator,
                           "MasterCounter":m.master_counter}
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
