"""
 cells, segments, nodes and channels connections.
"""
import pandas as pd
from io import StringIO 
import numpy as np
import h5py
from pydsm.input import parser

class gtm_grid:
   def __init__(self,hdf_fn,hydro_echo_fn):
      self.hdf_fn = hdf_fn
      self.hydro_echo_fn = hydro_echo_fn
      self._segments = None
      self._channels = None
      self._cells = None
      self._hydro_info = None
      self._cell_node = None
      self._node_cell = None
      self._node_rc = None
      self._ncell = None
      self._res_name = None
      self._nres = None

   @property
   def segments(self):
      if self._segments is None:
         self.read_gtm_h5()
      return self._segments

   @property
   def cells(self):
      if self._cells is None:
         self.cell_dataframe()
      return self._cells

   @property
   def channels(self):
      if self._hydro_info is None:
         self.read_hydro_echo()
      self._channels = self._hydro_info['CHANNEL']  
      return self._channels

   @property
   def rc(self):
      if self._hydro_info is None:
         self.read_hydro_echo()
      self._rc = self._hydro_info['RESERVOIR_CONNECTION']  
      return self._rc

   @property
   def ncell(self):
      if self._ncell is None:
         self._ncell = len(self.cells)
      return self._ncell

   @property
   def nres(self): 
      if self._nres is None:
         self._nres = len(self.res_name)
      return self._nres

   @property
   def res_name(self):
      if self._res_name is None:
         if self._hydro_info is None:
            self.read_hydro_echo()
         self._res_name = self._hydro_info['RESERVOIR'].NAME.values
      return self._res_name

   @property
   def cell_node(self):
      if self._cell_node is None:
         self.get_cell_node()
      return self._cell_node

   @property
   def node_cell(self):
      if self._node_cell is None:
         self.get_node_cell()
      return self._node_cell

   @property
   def node_rc(self):
      if self._node_rc is None:
         self.get_node_rc()
      return self._node_rc

   def read_gtm_h5(self):
      """
      Read a gtm output hdf5 file (any file)
      """
      gtm = h5py.File(self.hdf_fn,'r')
      df_seg = pd.DataFrame(gtm['geometry/segment'][()])
      df_seg['end_cell_no'] = df_seg[['start_cell_no','nx']].sum(axis=1)-1
      df_seg['delta_x'] = df_seg['length']/df_seg['nx']
      gtm.close()
      self._segments = df_seg

   def cell_dataframe(self):
      df_seg = self.segments
      ncell = int(df_seg.iloc[-1]['end_cell_no'])
      cells = np.linspace(1, ncell, ncell).astype(int)
      v = np.asarray([df_seg[(df_seg.start_cell_no<=i) & 
                          (df_seg.end_cell_no>=i) ][['segm_no','chan_num','chan_no']].values[0] for i in cells])
      segm_no = v[:,0]
      chan_num = v[:,1]
      chan_no = v[:,2]

      start_distance = []
      end_distance = []
      for i in cells:
         chan_i = chan_num[i-1]
         segm_i = segm_no[i-1]
         df_seg_i = df_seg[df_seg.segm_no==segm_i]
         dx_i = df_seg_i['delta_x'].values[0]    
         start_cell_no_i = df_seg_i['start_cell_no'].values[0]
         up_distance_i = df_seg_i['up_distance'].values[0]
         start_distance_i = (i - start_cell_no_i)*dx_i + up_distance_i
         start_distance.append(start_distance_i)
         end_distance.append(start_distance_i+dx_i)
      df_cell = pd.DataFrame()
      df_cell['cell']= cells
      df_cell['segm_no']=segm_no.astype(int)
      df_cell['chan_num'] = chan_num.astype(int)
      df_cell['chan_no'] = chan_no.astype(int)
      df_cell['start_distance'] = np.float32(start_distance)
      df_cell['end_distance'] = np.float32(end_distance)   
      self._cells = df_cell
      self._ncell = ncell

   def channel_to_cell(self, chan_no,distance):
      """
      convert from chan_no, distance pair to cell number
      distance could be a numeric value indicating distance
      or a string 'length' indicate the maximum length. 
      """
      df_cell = self.cells
      try:
         distance = np.float32(distance)
         if distance == 0:
            # note that 'chan_no' in input files = 'chan_num' in internal dsm2. 
            df_i = df_cell[ (df_cell.chan_num==chan_no) ].iloc[0] # use the first cell
            cell_no = int(df_i.cell)
         else: 
            df_i = df_cell[ (df_cell.chan_num==chan_no) & \
                            (df_cell.start_distance<distance) & \
                            (df_cell.end_distance>=distance)]
            cell_no = int(df_i.cell.values[0])
      except ValueError:
         if distance == 'length':
            df_i = df_cell[ (df_cell.chan_num==chan_no) ].iloc[-1] # use the last cell
            cell_no = int(df_i.cell)
         else:
            raise Exception('%s is not recognized in the obs_fn'%distance) 
      return cell_no

   def channel_to_cell_arr(self, chan_no_arr, distance_arr):
      cells = []
      for c, d in zip(chan_no_arr,distance_arr):
         cell_no = self.channel_to_cell(c,d)
         cells.append(cell_no)
      return cells     

   def obs_cells(self,fn,unique=False):
      """
      Read observational channel input file and find the cells corresponding
      to the channel number and distance pairs
      """
      df_obs = pd.read_csv(fn,skiprows=[0],comment='#',delim_whitespace=True)
      df_obs = df_obs[['NAME','CHAN_NO','DISTANCE']]
      df_obs = df_obs.iloc[0:-1] #remove the line with 'END'
      df_obs = df_obs.astype({'CHAN_NO':int})
      obs_cell = []
      chan_no = df_obs['CHAN_NO'].values
      distance = df_obs['DISTANCE'].values
      df_obs['cell'] = self.channel_to_cell_arr(chan_no,distance)

      if unique:  # if output unique cells are requred
         duplicated = df_obs.cell.duplicated(keep=False)
         df_obs['duplicated'] = duplicated
         if any(duplicated):
            print("Duplicated observational cell exists: will combine observed values \
                  for these cells")
            df = df_obs[df_obs.cell.duplicated(keep=False)]
            df_obs_unique = df_obs.groupby(by='cell').first()
            df_idx  = df.groupby(by='cell').apply(lambda x: 
               tuple(x.NAME)).reset_index(name='idx').set_index('cell')
            df_merged = df_obs_unique.join(df_idx)
            df_merged['cell'] = df_merged.index
            return df_merged
         else:
            return df_obs
      else:
         return df_obs

   def read_hydro_echo(self):
      """
      A function to read hydro_echo.inp file and generate a channal dataframe
      """
      with open(self.hydro_echo_fn, 'r') as file:
         tables = parser.parse(file.read())
      self._hydro_info = tables

   def get_cell_node(self):
      """
      Create a cell table with upnodes and downnodes
      """
      df_cell = self.cells
      channel_tables = self.channels
      cells = df_cell.cell.values
      cell_upnode = []
      cell_downnode = []
      for c in cells:
         upnode = -99
         downnode = -99
         df = df_cell[df_cell.cell==c]
         chan_num = df.chan_num
         cell_num = df.cell
         min_dist = 0.0
         max_dist = channel_tables[channel_tables.CHAN_NO==int(chan_num)].LENGTH.values[0]
         chan_upnode = channel_tables[channel_tables.CHAN_NO==int(chan_num)].UPNODE.values[0]
         chan_downnode = channel_tables[channel_tables.CHAN_NO==int(chan_num)].DOWNNODE.values[0]
         if df.start_distance.values == min_dist:
             upnode = chan_upnode
         if df.end_distance.values == max_dist:
             downnode = chan_downnode
         if upnode==-99: #upnode not assigned yet
             upnode = '%d_%d'%(chan_num,cell_num-1)
         if downnode ==-99: # downnode not assigned yet
             downnode = '%d_%d'%(chan_num,cell_num)
         cell_upnode.append(upnode)
         cell_downnode.append(downnode)
      df_cell['length'] = df_cell['end_distance']-df_cell['start_distance']        
      df_cell['upnode'] = cell_upnode
      df_cell['downnode'] = cell_downnode
      self._cell_node = df_cell


   def get_node_cell(self):
      """
      Create a node table with upcell and downcell. 
      """
      df_cell = self.cell_node
      nodes = list( set(df_cell.upnode.values).intersection(
         set(df_cell.downnode.values) ) ) # find overlapping nodes
      upcell = []
      downcell = []
      length = []
      conn = [] 
      for n in nodes:
         node_upcell = df_cell[df_cell.downnode==n].cell.values
         node_downcell = df_cell[df_cell.upnode==n].cell.values
         if len(node_upcell)==1 and len(node_downcell)==1:
            upcell.append(node_upcell[0])
            downcell.append(node_downcell[0])
            node_length = ( 
                df_cell[df_cell.cell==node_upcell[0]].length.values + 
                df_cell[df_cell.cell==node_downcell[0]].length.values 
                )/2.0
            length.append(node_length[0])
            conn.append(n)
         elif len(node_upcell)>1 and len(node_downcell)==1:
            upcell += node_upcell.tolist()
            downcell += [node_downcell[0]]*len(node_upcell)  
            node_length = (
                df_cell[df_cell.cell.isin(node_upcell)].length.values + 
                df_cell[df_cell.cell==node_downcell[0]].length.values
                )/2.0 
            length += node_length.tolist()     
            conn += [n]*len(node_upcell)     
         elif len(node_upcell)==1 and len(node_downcell)>1:
            upcell += [node_upcell[0]]*len(node_downcell) 
            downcell += node_downcell.tolist()
            node_length = (
                df_cell[df_cell.cell==node_upcell[0]].length.values + 
                df_cell[df_cell.cell.isin(node_downcell)].length.values
                )/2.0 
            length += node_length.tolist() 
            conn += [n]*len(node_downcell)
         else: # when multiple upcells and downcells exist       
            ud = np.array([ [u, d] for u in node_upcell for d in node_downcell]) 
            upcell += ud[:,0].tolist()
            downcell += ud[:,1].tolist()
            up_length = df_cell[df_cell.cell.isin(node_upcell)].length.values
            down_length = df_cell[df_cell.cell.isin(node_downcell)].length.values
            node_length = [0.5*(ul+dl) for ul in up_length for dl in down_length]
            length += node_length     
            conn += [n]*len(ud) 
            
      df_node_cells = pd.DataFrame({})
      df_node_cells['node'] = conn  # with multiple nodes for multiple connections
      df_node_cells['upcell'] = upcell
      df_node_cells['downcell'] = downcell
      df_node_cells['length'] = length
      self._node_cell = df_node_cells

   def get_node_rc(self):   
      """
      Create a table to map nodes to reservior
      set channel as upnodes and reservior as downnodes
      """    
      df_rc = self.rc
      df_cell = self.cell_node
      node = []
      chan_cell = []
      rc_cell = []
      for i in range(len(df_rc)):
         df = df_rc.iloc[i]
         rn = df.RES_NAME
         n = df.NODE
         chan_list = np.concatenate([
             df_cell[df_cell.upnode==n].cell.values, 
             df_cell[df_cell.downnode==n].cell.values,
             df_rc[df_rc.NODE==n].RES_NAME.values]   #include possible res to res connections
             ) 
         chan_list = set(chan_list) - set([rn])
         chan_cell += chan_list 
         rc_cell += [rn]*len(chan_list)
         node += [n]*len(chan_list)

      df_node_rc = pd.DataFrame({})
      df_node_rc['node'] = node
      df_node_rc['chan_cell'] = chan_cell
      df_node_rc['rc_cell'] = rc_cell
      self._node_rc =  df_node_rc.drop_duplicates()

   def get_obs_nodes(self, obs_fn):
      """
      Create an obs table with upnodes and downnodes
      """
      df_cell = self.cell_node
      df_obs = self.obs_cells(obs_fn,unique=True)      
      upnode = []
      downnode = []
      obs_names = df_obs.NAME.values
      for s in obs_names:
         df = df_obs[df_obs.NAME==s]
         cell_no = df.cell
         cell_upnode = df_cell[df_cell.cell==int(cell_no)].upnode.values[0]
         cell_downnode = df_cell[df_cell.cell==int(cell_no)].downnode.values[0]
         upnode.append(cell_upnode)
         downnode.append(cell_downnode)
      df_obs['upnode'] = upnode
      df_obs['downnode'] = downnode
      return df_obs


if __name__ == "__main__": 
   hdf_fn = '../output/historical_gtm.h5' 
   hydro_echo_fn = '../output/hydro_echo_pdc0_hydro_test.inp'


   gg = gtm_grid(hdf_fn,hydro_echo_fn)
   df_seg = gg.segments
   df_cell = gg.cells
   df_obs_plus = gg.obs_cells(obs_fn)
   #df_obs_plus.to_csv('obs.csv')
   df_channel = gg.channels

   df_node_cell = gg.node_cell
   df_node_rc = gg.node_rc
   df_node_obs = gg.get_obs_nodes(obs_fn)