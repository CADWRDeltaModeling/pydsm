=======
History
=======

2021.12.13
----------
Added major functionality for hydro and qual .h5 reading
Cleaned up documentation and notebook examples
Refactoring and testing 

2021.11.14
-----------
added normalized metrics

0.3.x (2020-12-24)
----------------
tidal amplitude
ptm animator changes
added stats functions
fixed for pyhecdss updates
added metrics functions

0.2.7 (2020-12-01)
------------------
added tests for tidal amplitude
added docs for dsm2 input reading/writing

0.2.6 (2020-10-16)
------------------
added ptm reading sample notebooks
added tidal high and low calculation function
added tsmath for period average/min/max using HEC style end of timestamp for consistency with HEC-DSSVue ops
added reservoir flow and qext flow retrieval from hdf5

0.2.5 (2020-04-09)
------------------
pinning conda recipe to use hdf5 1.10.4 and h5py to 2.10.x 

0.2.4 (2020-04-07)
------------------
updated mass balance with source flow reservoir

0.2.3 (2020-04-05)
------------------
switching to versioneer for versioning

0.2.0 (2020-03-17)
------------------
added input parsing of echo files into pandas DataFrame

added creation of graph network from channel and reservoir tables

0.1.9 (2019-11-11)
------------------
moved godin filter to vtools3. Dependency on vtools3>=3.0.1

0.1.8 (2019-10-28)
------------------
added command line for comparing dss files with tolerance

0.1.7 (2019-09-25)
------------------
fixed command line extract-dss error when monthly average is being calculated

0.1.6 (2019-09-24)
------------------
regression tested godin filter against vtools for 15 min time series

fix for installing command line pydsm script

0.1.5 (2019-09-19)
------------------
updated extract-dss command to used end of time period when resampling (min,max,avg commands)

0.1.4 (2019-09-16)
------------------
Reads hydro h5 flows and area

Godin filter time series

command line tool :

 extract_dss to postprocess to daily, max, min and monthly from DSM2 dss files

0.1.4 (2019-09-16)
------------------
Reads hydro h5 flows and area

Godin filter time series

command line tool :

 extract_dss to postprocess to daily, max, min and monthly from DSM2 dss files

0.1.0 (2019-08-21)
------------------

* No releases yet
