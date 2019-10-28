.. highlight:: shell

============
Installation
============


Stable release
--------------

To install pydsm, run this command in your terminal:

.. code-block:: console

    $ conda install -c cadwr-dms pydsm

This is the preferred method to install pydsm, as it will always install the most recent stable release.

Known Issues
------------
conda install h5py lacks szip/zlib support (https://github.com/ContinuumIO/anaconda-issues/issues/11382)
Instead conda uninstall h5py and pip install h5py instead


If you don't have `conda`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _conda: https://docs.conda.io/projects/conda/en/latest/user-guide/install/
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/

From sources
------------

The sources for pydsm can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone https://github.com/CADWRDeltaModeling/pydsm.git

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/CADWRDeltaModeling/pydsm/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/CADWRDeltaModeling/pydsm
.. _tarball: https://github.com/CADWRDeltaModeling/pydsm/tarball/master
