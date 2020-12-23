:: setting up development environment using conda
call conda create -y -n dev_pydsm python=3.7*
call conda install -y -n dev_pydsm -c cadwr-dms -c defaults pytest-runner pandas=1.0 numpy click pyhecdss vtools3 h5py=2.10 
call conda install -y -n dev_pydsm -c defaults networkx matplotlib autopep8 hvplot datashader geopandas
call conda install -y -n dev_pydsm -c defaults -c conda-forge ipywidgets ipyleaflet
call jupyter nbextension enable --py --sys-prefix ipyleaflet
call conda install -y -n dev_pydsm -c defaults -c conda-forge nbsphinx
conda activate dev_pydsm
pip install --no-deps -e .

