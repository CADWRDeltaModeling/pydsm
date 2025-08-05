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
from tabulate import tabulate


def read_input(filepath):
    """
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

    """
    with open(filepath, "r") as f:
        tables = parse(f.read())
    return tables


def write_input(filepath, tables, append=True):
    """
    Writes the input to the filepath all the tables (pandas DataFrame) in the dictionary

    Refer to read_input for the table format

    """
    with open(filepath, "a" if append else "w") as f:
        write(f, tables)


def quote_if_space(value):
    if isinstance(value, str) and " " in value:
        return f'"{value}"'
    return value


def pretty_print_to_handle(file_handle, table, tableName):
    """
    Write a formatted table to an open file handle.

    Parameters
    ----------
    file_handle : file object
        An open file handle to write to
    table : pandas DataFrame
        The table to be written
    tableName : str
        The name of the table
    """
    file_handle.write(tableName + "\n")
    try:
        table = table.map(quote_if_space)
        file_handle.write(
            tabulate(
                table,
                tablefmt="plain",
                headers=table.columns,
                showindex=False,
                colalign=("left",),
            )
        )
    except Exception as ex:  # colalign fails if no rows or for some other reason...
        file_handle.write(
            tabulate(table, tablefmt="plain", headers=table.columns, showindex=False)
        )
    file_handle.write("\n")
    file_handle.write("END\n")


def pretty_print(filepath, table, tableName, append=True):
    """
    Write a formatted table to a file.

    This function maintains backward compatibility. For more flexibility,
    use pretty_print_to_handle with an open file handle.

    Parameters
    ----------
    filepath : str
        Path to the file to write to
    table : pandas DataFrame
        The table to be written
    tableName : str
        The name of the table
    append : bool, default True
        Whether to append to the file or overwrite it
    """
    with open(filepath, "a" if append else "w") as f:
        pretty_print_to_handle(f, table, tableName)


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
    data = re.sub(re.compile("#.*?\n"), "\n", data)
    datatables = list(map(str.strip, re.split(r"END\s*\n", data)))
    tables = {}
    for table in datatables:
        with io.StringIO(table) as file:
            name = file.readline().strip()
            if name == "":
                continue
            try:
                df = pd.read_csv(file, sep=r"\s+", comment="#", skip_blank_lines=True)
                df = df.dropna()  # fix if last row is just END with nans
                tables[name] = df
            except Exception as ex:
                print(
                    "Exception reading: ",
                    name,
                )
                print(ex)
                print(table)
                raise
    return tables


def write(output, tables, pretty=True):
    """
    write to output handle (file or io.StringIO) the tables dictionary containing the names as keys and dataframes as values
    """
    for name in tables.keys():
        df = tables[name]
        if pretty:
            pretty_print_to_handle(output, df, name)
        else:
            output.writelines(name + "\n")
            tables[name].to_csv(output, index=None, sep=" ", lineterminator="\n")
            output.write("\nEND\n")


#
