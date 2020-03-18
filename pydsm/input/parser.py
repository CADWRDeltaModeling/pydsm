# Parses the DSM2 echo file into data frames 
# DSM2 input consists of table entries. The declaration of those look like
# <TABLE NAME>
# <COL_NAME1> <COL_NAME2>...<COL_NAMEN>
# <ROW1_COL1> <ROW1_COL2>..
# <ROW2_COL1> <ROW2_COL2>..
# END
import pandas as pd
import pydsm
import io
import re

def parse(data):
    """
    parse the data of the string to read in DSM2 input echo file
    returns a dictionary of tables with name as keys and dataframes as value
    """
    data=re.sub(re.compile("#.*?\n"),"",data)
    datatables=list(map(str.strip,re.split(r"END\s*\n",data)))
    tables={}
    for table in datatables:
        with io.StringIO(table) as file:
            name=file.readline().strip()
            try:
                df=pd.read_csv(file,sep=r'\s+',comment='#',skip_blank_lines=True)
                tables[name]=df
            except Exception:
                pass
    return tables

def write(output, tables):
    '''
    write to output handle (file or io.StringIO) the tables dictionary containing the names as keys and dataframes as values
    '''
    for name in tables.keys():
        df=tables[name]
        if not df.empty:
            output.write(name+'\n')
            tables[name].to_string(output,index=False,justify='left')
            output.write('\nEND\n')
#