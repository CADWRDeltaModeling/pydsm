#modify an existing gtm restart file to create a new restart file.

import numpy as np

base_restart = "../restart_gtm1000_nx2.qrf"
out_restart = "../restart_gtm1000_ones_nx2.qrt"

def read_restart(restart_fn,extract_details=False):
   with open(restart_fn,'r') as file:
      filedata = file.read()
   filetxt = filedata.split('\n')
   ncells = int(filetxt[3].split('/')[0])
   cell_header_str = '\n'.join(filetxt[0:5])
   cell_str = filetxt[5:5+ncells]
   cell_array = np.array([l.split() for l in cell_str])
   cell_name = cell_array[:,0]
   cell_value = cell_array[:,1]
   nreser =  int(filetxt[5+ncells].split('/')[0])
   res_header_str =  '\n'.join(filetxt[5+ncells:5+ncells+2])
   res_str = filetxt[5+ncells+2:5+ncells+2+nreser]
   res_array = np.array([l.split() for l in res_str])
   res_name =  res_array[:,0]
   res_value = res_array[:,1]
   if extract_details: 
      return cell_name, res_name, ncells, nreser,cell_header_str,res_header_str
   else:
      field = np.append(cell_value,res_value)
      return field


def write_restart(field,restart_fn,cell_name, res_name, 
     cell_header_str, res_header_str):
   """
   read from a pdaf field_fn and write into a gtm restart_fn
   """   
   #field = read_pdaf_field(field_fn)
   ncells = len(cell_name)
   nres = len(res_name)
   cell_list = ['%32s%32s'%(cid,cv) for cid, cv in zip(cell_name, field[:ncells])]
   cell_str = '\n'.join(cell_list)
   res_list = [cid.ljust(32) + '%32s'%cv for cid, cv in zip(res_name,field[ncells:ncells+nres])]
   res_list = '\n'.join(res_list)
   field_str = '\n'.join([cell_header_str,cell_str,res_header_str,res_list])
   with open(restart_fn,'w') as file:
      file.write(field_str)

# Get basic structure from an example restart file. 
cell_name, res_name, ncells, nreser,cell_header_str,res_header_str = read_restart(base_restart,extract_details=True) 
field = read_restart(base_restart)
field_new = np.ones_like(field)   # all one initial condition 
#field_zero = (field.astype(float)*0.0).astype(str)
write_restart(field_new,out_restart,cell_name,res_name,
   cell_header_str, res_header_str)

