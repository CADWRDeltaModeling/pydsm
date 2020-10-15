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
import csv

def read_input(filepath):
    '''
    reads input from a filepath and returns a dictionary of pandas DataFrames.
    
    Each table in DSM2 input is mapped to a data frame. The name of the table is key in the dictionary

    e.g.

    ::
        CHANNEL
        CHAN_NO  LENGTH  MANNING  DISPERSION  UPNODE  DOWNNODE  
        1        19500   0.0350    360.0000    1       2    
        ...
        END


    The above table will be parsed as pandas DataFrame with key 'CHANNEL' in the returned dictionary

    '''
    with open(filepath,'r') as f:
        tables=parse(f.read())
    return tables

def write_input(filepath, tables, append=True):
    '''
    Writes the input to the filepath all the tables (pandas DataFrame) in the dictionary

    Refer to read_input for the table format

    '''
    with open(filepath,'a' if append else 'w') as f:
        write(f,tables)
    
def parse(data):
    """
    parse the data of the string to read in DSM2 input echo file
    returns a dictionary of tables with name as keys and dataframes as value

    Parameters
    ----------
    data : string 
        contents to be parsed

    Examples
    ----------
    >>> fname='../tests/hydro_echo_historical_v82.inp'
    >>> with open(fname, 'r') as file:
      tables = parser.parse(file.read())

    Returns
    ---------
    dict of pandas DataFrame: with table name as the key
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
            except Exception as ex:
                print('Exception reading: ',name)
                print(ex)
                print(table)
                pass
    return tables

def write(output, tables):
    '''
    write to output handle (file or io.StringIO) the tables dictionary containing the names as keys and dataframes as values
    '''
    for name in tables.keys():
        df=tables[name]
        output.write(name+'\n')
        tables[name].to_csv(output,index=None,sep=' ',line_terminator='\n')
        output.write('\nEND\n')
#