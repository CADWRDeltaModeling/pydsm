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

DSM2 Diff
~~~~~~~~~

Compare two DSM2 studies by their hydro echo ``.inp`` files.  The tool reports:

* structural differences in every input table (added, removed, and changed rows)
* RMSE and bias for DSS-backed time-series inputs (boundary conditions, operating rule time series, etc.)
* a consolidated "missing rows" summary across all tables

**Minimal example — self-contained structural diff only (no DSS loading):**

.. code-block:: bash

    pydsm diff \
        studies/baseline/output/hydro_echo_baseline.inp \
        studies/scenario1/output/hydro_echo_scenario1.inp \
        --no-csv

**Default run — structural diff + time-series comparison for the three default TS tables:**

The default TS tables are ``BOUNDARY_FLOW``, ``BOUNDARY_STAGE``, and ``OPRULE_TIME_SERIES``.  The
comparison window defaults to the intersection of both studies' ``run_start_date`` /
``run_end_date`` scalars.  Results are also written to CSV files in the current directory.

.. code-block:: bash

    pydsm diff \
        studies/baseline/output/hydro_echo_baseline.inp \
        studies/scenario1/output/hydro_echo_scenario1.inp \
        --outdir diff_results/

**Restrict to a specific time window:**

.. code-block:: bash

    pydsm diff \
        studies/baseline/output/hydro_echo_baseline.inp \
        studies/scenario1/output/hydro_echo_scenario1.inp \
        --timewindow "01JAN2021 0000 - 31DEC2022 0000" \
        --outdir diff_results/

**Compare additional TS tables (e.g. include source flows):**

``SOURCE_FLOW`` has over 1 000 entries; use ``--force`` to bypass the default 25-row guard.

.. code-block:: bash

    pydsm diff \
        studies/baseline/output/hydro_echo_baseline.inp \
        studies/scenario1/output/hydro_echo_scenario1.inp \
        --tables boundary_flow \
        --tables boundary_stage \
        --tables oprule_time_series \
        --tables source_flow_reservoir \
        --outdir diff_results/

    # include SOURCE_FLOW (large table — requires --force)
    pydsm diff \
        studies/baseline/output/hydro_echo_baseline.inp \
        studies/scenario1/output/hydro_echo_scenario1.inp \
        --tables source_flow \
        --force \
        --outdir diff_results/

**All TS tables at once:**

.. code-block:: bash

    pydsm diff \
        studies/baseline/output/hydro_echo_baseline.inp \
        studies/scenario1/output/hydro_echo_scenario1.inp \
        --all-tables \
        --force \
        --outdir diff_results/

**Adjust the RMSE significance threshold:**

By default any RMSE below ``0.01`` is not flagged.  To tighten or relax this:

.. code-block:: bash

    pydsm diff study_a.inp study_b.inp --threshold 1.0 --outdir diff_results/

**Full option reference:**

.. code-block:: text

    Usage: pydsm diff [OPTIONS] ECHO_A ECHO_B

      Compare two DSM2 studies by their hydro echo files.

    Arguments:
      ECHO_A   Path to study A hydro echo .inp file.
      ECHO_B   Path to study B hydro echo .inp file.

    Options:
      -t, --tables TABLE       DSS-backed tables to diff (repeatable).
                               Default: BOUNDARY_FLOW BOUNDARY_STAGE
                               OPRULE_TIME_SERIES
      --all-tables             Diff all known TS-backed tables.
      --timewindow TEXT        "01JAN2020 0000 - 01JAN2022 0000".
                               Default: intersection of both run periods.
      --threshold FLOAT        RMSE below this is not reported.  [default: 0.01]
      --max-ts INTEGER         Skip DSS loading for tables > this many rows.
                               [default: 25]
      --force                  Load DSS even when table exceeds --max-ts.
      --outdir PATH            Directory for CSV output files.  [default: .]
      --no-csv                 Suppress CSV output; terminal only.
      -h, --help               Show this message and exit.

**Output CSV files:**

When differences are found the following files are written to ``--outdir``:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - File
     - Contents
   * - ``<table>_added.csv``
     - Rows present in study B only
   * - ``<table>_removed.csv``
     - Rows present in study A only
   * - ``<table>_changed.csv``
     - Rows with same key but different values (``*_a`` / ``*_b`` columns + ``changed_cols``)
   * - ``<table>_ts_summary.csv``
     - Per-entry RMSE, bias, path_match, n_points
   * - ``<table>_<name>_diff.csv``
     - Timestamps + ``a``, ``b``, ``diff`` columns for entries where RMSE > threshold
   * - ``<table>_missing.csv``
     - Entries present in only one study

**Using the Python API directly:**

.. code-block:: python

    from pydsm.analysis.dsm2diff import DSM2Diff

    d = DSM2Diff(
        "studies/baseline/output/hydro_echo_baseline.inp",
        "studies/scenario1/output/hydro_echo_scenario1.inp",
    )

    # Full run (default TS tables, auto timewindow)
    report = d.run()
    report.print_report()
    report.to_csv("diff_results/")

    # Targeted: check a single static table
    sd = d.diff_static("CHANNEL")
    print(sd.summary_line())   # e.g. "-3 removed, ~2 changed"
    sd.changed                 # pandas DataFrame with *_a / *_b value columns

    # Targeted: check one TS table with explicit timewindow
    ts = d.diff_ts_table(
        "BOUNDARY_FLOW",
        timewindow="01JAN2021 0000 - 01JAN2024 0000",
        threshold=0.1,
    )
    ts.summary                 # DataFrame: name | path_match | rmse | bias | n_points
    ts.diff_series["sac"]      # DataFrame: timestamp | a | b | diff

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
