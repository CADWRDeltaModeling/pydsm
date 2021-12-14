=====
pydsm
=====

For working with DSM2 input (.inp) and output (.h5 and .dss) with python (pandas)

* Free software: MIT license
* Documentation: https://cadwrdeltamodeling.github.io/pydsm


Features
--------

* Reads DSM2 hdf5 files into pandas DataFrame
    * Reads Hydro tidefiles time series data, input tables and geometry tables
    * Reads Qual tidefiles time series data and input tables
* Reads and writes DSM2 echo files into and from pandas DataFrame
* Shows use of graph network libraries to work with DSM2 grid as a graph of nodes and channels/reservoirs connecting them

Known Issues
------------

conda install h5py lacks szip/zlib support (https://github.com/ContinuumIO/anaconda-issues/issues/11382)
Instead conda uninstall h5py and pip install h5py instead

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
