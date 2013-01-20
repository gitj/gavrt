"""
.. automodule:: personalities.IbobPersonality

.. automodule:: personalities.TwoPolCospec

.. automodule:: personalities.TwoPolDDC

.. automodule:: personalities.OnePolReal

.. automodule:: personalities.DedispSpec
"""

from IbobPersonality import IbobPersonality, DummyPersonality
from TwoPolCospec import Cospec
from TwoPolDDC import TwoPolDDCSpectrometer
from OnePolReal import OnePolReal512ChannelKurtosisSpectrometer, OnePolReal512ChannelSpectrometer, OnePolRealKurtosisSpectrometer, OnePolRealSpectrometer
from DedispSpec import DDCDedisp, WideX4Dedisp

# would be good to make the following automatically generated
personalitiesList = ['DummyPersonality', 'Cospec', 'TwoPolDDCSpectrometer', 
                     'OnePolReal512ChannelKurtosisSpectrometer',
                     'OnePolReal512ChannelSpectrometer',
                     'OnePolRealKurtosisSpectrometer',
                     'OnePolRealSpectrometer',
                     'DDCDedisp',
                     'WideX4Dedisp'
                     ]
