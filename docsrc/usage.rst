=====
Usage
=====

To use pydsm in a project::

    import pydsm

Command Line 
------------

To use pydsm from command line to compare files::

    Usage: pydsm [OPTIONS] COMMAND [ARGS]...

    Options:
    --help  Show this message and exit.

    Commands:
    compare-dss  Compares the dss files for common pathnames (B and C parts)...
    extract-dss  Extract data from DSS file, optionally filtering it and...
    ptm-animate

Compare DSS
~~~~~~~~~~~~

    Usage::

        pydsm compare-dss [OPTIONS] DSSFILE1 DSSFILE2

        Compares the dss files for common pathnames (B and C parts)  after
        filtering for matching c parts and compare values with tolerance (default
        of 3 digits)

        Options:
        --cpart TEXT            filter by cpart string match (e.g. EC for only loading EC)
        --threshold FLOAT       Threshold to check for mean squared error
        --threshold-metric      [mean_error|mse|rmse|nash_sutcliffe|percent_bias]
        --metricsfile TEXT      name of file to write out metrics differnce
        --time-window TEXT      ddMMMyyyy [HHmm] - ddMMMyyyy [HHmm], e.g. "01JAN1990 - 01OCT1991" (quoted on command line)
        --help                  Show this message and exit.

Extract DSS 
~~~~~~~~~~~~
    Usage::

        pydsm extract-dss [OPTIONS] DSSFILE

        Extract data from DSS file, optionally filtering it and writing to a
        pickle for quick future loads

        Options:
        -o, --outfile TEXT        path to output file (ends in .zip, .gz, .bz2 for
                                    compression), (.h5 for hdf5), (.dss for dss)

        --cpart TEXT              filter by cpart string match (e.g. EC for only
                                    loading EC)

        -godin, --godin-filter    apply godin filter before writing out
        -davg, --daily-average    average to daily values
        -dmax, --daily-max        maximum daily value
        -dmin, --daily-min        minimum daily value
        -mavg, --monthly-average  monthly average value
        --help                    Show this message and exit.

PTM Animate
~~~~~~~~~~~~

    To use pydsm to run ptm animator. 

    Usage::

        pydsm ptm-animate [OPTIONS] PTM_FILE HYDRO_FILE FLOWLINES_SHAPE_FILE

        Options:
        --help  Show this message and exit.pydsm ptm-animate

Slice Hydro
~~~~~~~~~~~

    Slices hydro tidefile and writes out a hydro tidefile

    Usage:: 
    
        pydsm slice-hydro [OPTIONS] INFILE OUTFILE STIME ETIME

        Slices hydro tidefile and writes out a hydro tidefile

        Args:

            INFILE (str): Input hydro tidefile

            OUTFILE (str): Output hydro tidefile

            STIME (str): Datetime string, e.g. 1990-01-10

            ETIME (str): Datetime string, e.g. 1990-01-15

        Options:
        --help  Show this message and exit.
