import h5py
import numpy as np

hydro_path = r'd:\delta\dsm2_studies\studies\historical\output\hist_fc_mss.h5'

with h5py.File(hydro_path, 'r') as f:
    # --- channel flow structure (the target format to match) ---
    ds = f['hydro/data/channel flow']
    print(f"channel flow: shape={ds.shape}, dtype={ds.dtype}")
    print(f"  attrs: interval={ds.attrs['interval']}, start_time={ds.attrs['start_time']}")
    print()

    # --- inst device flow (gate/device time series already in HYDRO) ---
    ds2 = f['hydro/data/inst device flow']
    print(f"inst device flow: shape={ds2.shape}, dtype={ds2.dtype}")
    print(f"  attrs: {dict(ds2.attrs)}")
    print()

    # --- /hydro/input/gate (static input table) ---
    gi = f['hydro/input/gate']
    if isinstance(gi, h5py.Dataset):
        print(f"/hydro/input/gate Dataset: shape={gi.shape}, dtype={gi.dtype}")
        print(f"  first 5 rows: {gi[:5]}")
    else:
        print("/hydro/input/gate is a Group:")
        for k in gi.keys():
            v = gi[k]
            print(f"  {k}: shape={v.shape}, dtype={v.dtype}")
    print()

    # --- /hydro/input/gate device ---
    if 'hydro/input/gate device' in f:
        gd = f['hydro/input/gate device']
        if isinstance(gd, h5py.Dataset):
            print(f"/hydro/input/gate device Dataset: shape={gd.shape}, dtype={gd.dtype}")
            print(f"  first 5 rows: {gd[:5]}")
    print()

    # --- list all /hydro/geometry/ ---
    print("=== /hydro/geometry/ ===")
    for k in f['hydro/geometry'].keys():
        v = f['hydro/geometry'][k]
        print(f"  {k}: shape={v.shape}, dtype={v.dtype}")

