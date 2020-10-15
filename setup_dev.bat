# setting up development environment using conda
conda create -y -n dev_pydsm python=3.7*
conda install -y -n dev_pydsm -c cadwr-dms -c defaults pytest-runner pandas numpy click pyhecdss vtools3 h5py
conda install -y -n dev_pydsm -c defaults networkx matplotlib auto-pep8 hvplot
conda install -y -n dev_pydsm -c defaults -c conda-forge ipywidgets ipyleaflet
jupyter nbextension enable --py --sys-prefix ipyleaflet
conda install -y -n dev_pydsm -c defaults -c conda-forge nbsphinx
