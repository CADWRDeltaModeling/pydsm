# test szip
import h5py
import os

def test_szip_available():
    with h5py.File("test.h5", "w") as h5f:
        ds = h5f.create_dataset("test", (1024, 1024), chunks=(16,16), dtype="uint16", compression="szip", compression_opts=('ec',32))
        assert ds.compression == "szip"
    # Optional: Clean up the created file after the test
    if os.path.exists("test.h5"):
        os.remove("test.h5")