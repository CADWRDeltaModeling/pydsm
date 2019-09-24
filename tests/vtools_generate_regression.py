'''
This file is meant for python 2.7 and vtools package.
The main aim was to generate a regression dss file to compare against for godin filter
'''
from vtools.datastore.dss.api import *
from vtools.functions.api import *
from vtools.data.api import *
from vtools.functions.filter import *
file="justoneone.dss"
ts=dss_retrieve_ts(file,selector="B=ONES F=TEST",unique=True)
ts2=godin(ts)
dss_store_ts(ts2,file,"/TEST/ONES/ALL//15MIN/TEST-VTOOLS-GODIN/")
ts3=cosine_lanczos(ts,cutoff_period=hours(40))
dss_store_ts(ts3,file,"/TEST/ONES/ALL//15MIN/TEST-VTOOLS-LANCZOS/")