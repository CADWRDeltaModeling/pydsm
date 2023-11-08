# Check mass conservations from output flow of hydro hdf5 file for a predefined nquadpts. 

import numpy as np
import pandas as pd
import h5py


hdf_fn = "../output/pdc0_hydro_test.h5"
gtm_fn = "../output/historical_gtm_1000.h5"
Theta = 0.6
DT = 5*60
nquadpts = 3

if nquadpts==1:
    QuadWts = [0.5]
    QuadPts = [1.0]
elif nquadpts==2:
    QuadWts = [0.5, 0.5]
    QuadPts = [0.0, 1.0]
else:
    QuadWts = [0.25, 0.5, 0.25]
    QuadPts = [0.0, 0.5, 1.0]

hydro = h5py.File(hdf_fn,'r')
df_comp_point = pd.DataFrame(hydro['hydro/geometry/hydro_comp_point'][()])
df_inst_flow = pd.DataFrame(hydro['hydro/data/inst flow'][()],dtype='double')
df_inst_ws = pd.DataFrame(hydro['hydro/data/inst water surface'][()],dtype='double')
start_time = hydro['hydro'].attrs['Start time string'].decode()
time_interval = '%smin'%hydro['hydro'].attrs['Time interval'][0]
run_time = pd.date_range('2021-01-01', periods=len(df_inst_flow), freq=time_interval)

gtm = h5py.File(gtm_fn,'r')
df_seg = pd.DataFrame(gtm['geometry/segment'][()])

chan_no = pd.DataFrame(hydro['hydro/geometry/channel_number'][()])
df_channel = chan_no.rename(columns={0:'chan_no'})
df_channel.loc[:,'channel']= df_channel.index.values
df_comp_point = df_comp_point.merge(df_channel,on='channel')

xsect = pd.DataFrame(hydro['hydro/geometry/virtual_xsect'][()])

df_channel = pd.DataFrame(hydro['hydro/input/channel'][()])

def calculateChannelGeometryAspects(X, Z,channo,channel):
    # I have checked this function and its output Area is consistent
    # with the way hydro calculates area. 
    # This function is consistent with CxInfo from GTM and calculateChannelGeometryAspects in hydro in its way to calculate area. 
    vcsection = xsect.query("chan_no==%s"%channel)
    num_virt_sec = vcsection.iloc[0].num_virt_sec
    chan_length = df_channel.iloc[channel-1].length
    virt_deltax = chan_length / (num_virt_sec-1)
    vsecno = np.floor(X/virt_deltax)+1  #index number for the cross-section
    vcsection_v = vcsection.query("vsecno==%s"%vsecno)
    virt_elevation = vcsection_v.elevation.values
    virt_area = vcsection_v.area.values
    virt_width = vcsection_v.width.values
    minelev = vcsection_v.min_elev.values[0]
    maxelev = virt_elevation.max()

    if (Z<minelev).any():
        raise(" Error...channel %s dried up at time or runtime instability developed; WS Elevation Z=%s"%(channo, Z))
    elif (Z>maxelev).any():
        raise("Error in find_layer_index in channel %s"%channo)
    else:
        f = lambda x: np.where(virt_elevation<x)[0][-1] # the last index where elev<x        
        fv = np.vectorize(f)
        veindex = fv(Z)  # the first virtual elevation above Z. 
        Z1=virt_elevation[veindex]
        Z2=virt_elevation[veindex+1]
        dZ=Z2-Z1
        slope=(Z-Z2)/dZ
        y1=virt_width[veindex]
        y2=virt_width[veindex+1]       
        ChannelWidth = (y2-y1)*slope+y2 #interp(x1,x2,y1,y2,Z)
        a1=virt_area[veindex]
        b1=virt_width[veindex]
        b2=ChannelWidth
        CxArea = a1+(0.5*(b1+b2))*(Z-Z1)
        print(veindex)
        
    return ChannelWidth, CxArea

#Find the channel 201
comp_index = int(df_comp_point.query('chan_no==201').iloc[0]['comp_index'])
 # note that it is one-based. 
df_comp_point.query('chan_no==201')
seg_no = 966
Up = df_seg.query('segm_no==%d'%seg_no).up_comp.values[0]
Down = df_seg.query('segm_no==%d'%seg_no).down_comp.values[0]


Q1 = df_inst_flow[Up-1].values
Z1 = df_inst_ws[Up-1].values

Q2 = df_inst_flow[Down-1].values
Z2 = df_inst_ws[Down-1].values

channo = int(df_seg.query('segm_no==%d'%seg_no).chan_num.values[0])  # this is the channo in dsm2
channel = int(df_seg.query('segm_no==%d'%seg_no).chan_no.values[0])  # this is the index number for channels. 

X1 = df_comp_point.query('comp_index==%s'%Up).distance.values[0]
Width1, Area1 = calculateChannelGeometryAspects(X1,Z1,channo,channel)
X2 = df_comp_point.query('comp_index==%s'%Down).distance.values[0]
Width2, Area2 = calculateChannelGeometryAspects(X2,Z2,channo,channel)
DX = X2-X1

# LHS of the mass conservation equation:
# Coef(1)*Q1 + Coef(2)*Z1 + Coef(3)*Q2 + Coef(4)*Z2 = MassConst + MassAdjust
Coef_1 = - Theta * DT
Coef_2 =   0.0
Coef_3 = - Coef_1
Coef_4 =   0.0
Volume = 0.0

for QuadPt, QuadWt in zip(QuadPts, QuadWts):
    N_1 = 1.0 - QuadPt
    N_2 = QuadPt            
    Xdist = N_1 * X1 + N_2 * X2            
    Q = N_1 * Q1 + N_2 * Q2
    Z = N_1 * Z1 + N_2 * Z2
    
    if abs(Xdist-X1) < 1e-06:
        Width=Width1
        Area=Area1
    elif abs(Xdist-X2) < 1e-06:
        Width=Width2
        Area=Area2
    else:
        Width, Area = calculateChannelGeometryAspects(Xdist,Z,channo,channel)

    Coef_2 = Coef_2 + QuadWt*DX*Width*N_1
    Coef_4 = Coef_4 + QuadWt*DX*Width*N_2
    Volume += QuadWt * Area * DX

# Compare if the two way the mass is calculated is correct. 
# Current_volume = Coef_2[1:]*Z1[1:] + Coef_4[1:]*Z2[1:] 
mass_fluxes = Theta*DT*(Q1[1:] - Q2[1:]) + ( (1- Theta)*(Q1[:-1] -Q2[:-1])*DT)
 
mass_closure_error =  mass_fluxes - np.diff(Volume)
flow_volume_change = mass_fluxes
area_volume_change = np.diff(Volume)

