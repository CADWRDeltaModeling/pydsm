=====
pydsm
=====


For loading data from DSM2 hdf5 files into pandas DataFrame


* Free software: MIT license
* Documentation: https://cadwrdeltamodeling.github.io/pydsm


Features
--------

* Reads DSM2 hdf5 files into pandas DataFrame
* Reads DSM2 echo files into pandas DataFrame

Known Issues
------------
conda install h5py lacks szip/zlib support (https://github.com/ContinuumIO/anaconda-issues/issues/11382)
Instead conda uninstall h5py and pip install h5py instead

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
