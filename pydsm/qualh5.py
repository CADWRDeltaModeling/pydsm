'''
Qual H5 reader. 

'''
import h5py
import pandas as pd
import numpy as np
from .import h5common

class QualH5:
    '''
    DSM2 Qual HDF5
    ---------------
    DSM2 outputs water quality constituents in HDF5 format. 

    The outputs are at:

    - output:
    
     - /constiuent_names
     - /channel concentration
     - /channel_number

    
    '''
    def __init__(self, filename):
        '''
        opens a handle to the filename in read mode
        '''
        self.h5=h5py.File(filename,'r')

    def __del__(self):
        '''
        closes file as cleanup
        '''
        self.h5.close()

    def get_constituents(self):
        '''
        get list of constituents
        '''
        df=pd.DataFrame(self.h5.get('output/constituent_names'))
        df[0]=df[0].str.decode('utf-8')
        df.columns=['constituent_names']
        return df

    def get_channel_concentration(self):
        '''
        get channel concentration table
        '''
        tsdata=self.h5.get('output/channel concentration')
        attrs={k: tsdata.attrs[k] for k in tsdata.attrs.keys()}

    def get_channel_numbers(self):
        '''
        get index to channel number table
        '''
        df=pd.DataFrame(self.h5.get('output/channel_number'))
        df.columns=['channel_number']
        return df
