'''
This module contains functions for Particle Tracking Model 
'''
import pandas as pd
import numpy as np
def load_ptm_trace(trace_file):
    '''
    Loads the trace file output from PTM as a pandas dataframe and associate meta data as a dictionary
    returns the data frame and a dictionary containing the start_date, end_date, timestep and number of particles 


    Example Usage: 
    df,meta_info=load_ptm_trace('./dsm2_v8.2.0b1/studies/historical/output/trace.out')

    '''
    column_names=['julianmin','particle_id','node_id','waterbody_id']
    df=pd.read_csv(trace_file,sep=r'\s+',names=column_names)
    t0=pd.Timestamp('1899-12-31')
    row0=df.iloc[0]
    start_date=t0+pd.to_timedelta(row0.julianmin,'m')
    end_date=t0+pd.to_timedelta(row0.particle_id,'m') # really just misnamed
    timestep=row0.node_id
    nparticles=row0.waterbody_id
    df=df[1:]
    df.index=t0+pd.to_timedelta(df['julianmin'],'m')
    df.index.name='datetime'
    return df,{'start_date':start_date,'end_date':end_date,'timestep':timestep,'nparticles':nparticles}

# Reads ptm animation binary file. needs swap to small endians
def load_anim_data(ptm_file):
    '''
    Reads the PTM Animation binary file format. This is Java binary output file and here numpy is used to 
    load all the records into a numpy structure and returns that

    '''
    ptm_meta=np.core.records.fromfile(ptm_file,dtype=[('x','>h'), ('model_date','a9'), ('model_time','>h'), ('nparticles','>h')], shape=1)
    nparticles=ptm_meta.nparticles[0]
    ptm_data=np.core.records.fromfile(ptm_file,dtype=[('x','>h'), ('model_date','a9'), ('model_time','>h'), ('nparticles','>h'),('positions','(%d,6)>h'%nparticles)])
    ptm_data=ptm_data.byteswap().newbyteorder() # Needed for converting big endianess to small endianess
    return ptm_data

def create_frame_for_anim_data(ptm_data):
    '''
    Creates a pandas dataframe from the numpy array data structure
    The data frame is multi indexed with an index of datetime and particle id 
    and the columns have the channel id , x,y,z position and the value field
    '''
    dt=[ pd.to_datetime(p.model_date.decode('utf-8')) + pd.to_timedelta(p.model_time // 100,unit='h') + pd.to_timedelta(p.model_time % 100, unit='m') for p in ptm_data ]
    ptm_dt_index=pd.MultiIndex.from_product([dt,range(ptm_data[0].nparticles)],names=['datetime','id'])
    dfall=pd.DataFrame(ptm_data.positions.reshape(len(ptm_data)*ptm_data[0].nparticles,6),index=ptm_dt_index,columns=['id','cid','x','y','z','val'])
    return dfall