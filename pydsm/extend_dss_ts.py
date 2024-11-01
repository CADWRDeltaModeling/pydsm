# %%
import pandas as pd
import pyhecdss as dss


# %%
# Extends time series data by shifting by a number of days
def extend_ts(df, days):
    """
    Extends a time series by shifting it by a number of days

    Parameters
    ----------
    df : DataFrame
        pandas DataFrame with a datetime index
    days : int
        number of days to shift the data

    Returns
    -------
    DataFrame
        pandas DataFrame with the data shifted by days
    """
    try:
        return df.shift(days, freq="D")
    except Exception as e:
        print(
            f"Exception {e} : Could not extend time series data. attempting to shift by 12 months"
        )
        return df.shift(12, freq="M")


# %%
import click


@click.command()
@click.argument(
    "dss_filename",
    type=click.Path(exists=True),
    required=True,
)
@click.argument("dss_ext_filename", type=click.Path(), required=True)
@click.option(
    "--days", type=int, default=366, help="Number of days to extend the time series"
)
@click.option(
    "--pathfilter", type=click.STRING, default="///////", help="HECDSS path filter"
)
def extend_dss_ts(dss_filename, dss_ext_filename, days=366, pathfilter="///////"):
    """
    Extends time series data in a DSS file by a number of days

    Parameters
    ----------
    dss_filename : str
        DSS filename to read the time series data from
    dss_ext_filename : str
        DSS filename to write the extended time series data to
    days : int, optional
        number of days to extend the time series, by default 366
    pathfilter : str, optional
        HECDSS path filter, by default "///////"
    """
    with dss.DSSFile(dss_ext_filename, create_new=True) as dhout:
        for df, units, ptype in dss.get_matching_ts(dss_filename, pathfilter):
            dfn = extend_ts(df, days)
            dfn = df.combine_first(dfn)
            p = df.columns[0]
            if p.split("/")[5].startswith("IR-"):
                dhout.write_its(p, dfn, units, ptype)
            else:
                dhout.write_rts(p, dfn, units, ptype)
