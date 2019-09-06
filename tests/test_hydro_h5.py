import pydsm.io
filename='../data/output/historical_v82.h5'
def test_get_model():
    assert pydsm.io.get_model(filename) == 'hydro'
def test_list_paths():
    table_paths=pydsm.io.list_table_paths(filename)
    print(table_paths)
def test_read_table_attr():
    attr=pydsm.io.read_table_attr(filename,'/hydro/data/channel flow')
    print(attr)
