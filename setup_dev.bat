# setting up development environment using conda
conda create -y -n dev_pydsm python=3.7*
conda install -y -n dev_pydsm -c defaults -c cadwr-dms pytest-runner pandas numpy click pyhecdss vtools3
# conda install h5py does not have support for szip by the pip install via pypi does. hence the install via pip below
pip install h5py
conda install -y -n dev_pydsm networkx matplotlib auto-pep8 hvplot
conda install -y -n dev_pydsm -c defaults -c conda-forge ipywidgets ipyleaflet
jupyter nbextension enable --py --sys-prefix ipyleaflet
conda install -y -n dev_pydsm -c defaults -c conda-forge nbsphinx