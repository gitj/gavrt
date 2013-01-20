"""
Wrapper class for LabJack u3 to fix shortcomings in existing class
"""
from u3 import *
from u3 import U3 as baseU3

class U3(baseU3):
    def setFIODir(self,io,dir):
        return self.getFeedback(BitDirWrite(io,dir))
        

