'''
GTM H5 reader. 

'''
import h5py
import pandas as pd
import numpy as np

from . import dsm2h5


class QualH5:
    '''
    DSM2 GTM HDF5
    ---------------
    DSM2 outputs water quality constituents in HDF5 format. 

    The outputs are at:

    - output:

     - cell concentration
     - constituent_names
     - reservoir concentration
     - reservoir_names

    '''

    def __init__(self, filename, filemode='r'):
        '''
        opens a handle to the filename in read mode
        '''
        self.h5 = h5py.File(filename, filemode)
        if not dsm2h5.get_model(self.h5) == 'gtm':
            raise f'{filename} is not a gtm tidefile: Could be '+dsm2h5.get_model(self.h5)+' ?'
        # initialization of tables needed
        self.constituents = self.get_constituents()

    def get_data_tables(self):
        return [s.split('/')[2] for s in dsm2h5._MODEL_TO_DATA_PATHS_MAP['gtm']]

    def get_constituents(self):
        '''
        get list of constituents
        '''
        return dsm2h5.read_table_as_df(gh5, '/output/constituent_names').iloc[:,0].values.tolist()

    def 