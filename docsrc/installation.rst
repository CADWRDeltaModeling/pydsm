.. highlight:: shell

============
Installation
============


Stable release
--------------

To install pydsm, run this command in your terminal:

.. code-block:: console

    $ conda env create -f enviornment.yml
    or install into a current environment 
    $ conda install -c cadwr-dms pydsm

This is the preferred method to install pydsm, as it will always install the most recent stable release.

Known Issues
------------
conda install h5py lacks szip/zlib support (https://github.com/ContinuumIO/anaconda-issues/issues/11382)
Instead conda uninstall h5py and conda install -c cadwr-dms h5py


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
    $ cd pydsm 
    $ conda env create -f environment_dev.yml
    $ pip install --no-deps -e .

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/CADWRDeltaModeling/pydsm/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ pip install --no-deps -e .


.. _Github repo: https://github.com/CADWRDeltaModeling/pydsm
.. _tarball: https://github.com/CADWRDeltaModeling/pydsm/tarball/master
