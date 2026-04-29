import click
import sys
import pandas as pd
import pyhecdss as dss
from pydsm.output.utils import write_csv_with_meta


def get_bpart_pattern():
    return '[0-9]+|BBID'


def bparts_to_pattern(bparts):
    """
    Convert a list of B-part strings to a regex alternation pattern for DSS path matching.
    Each B-part is wrapped in ^ and $ anchors so that e.g. '1' does not match '11' or '100'.
    (pyhecdss uses pandas str.match which anchors the start but not the end.)

    Parameters
    ----------
    bparts : list of str
        List of B-part identifiers, e.g. ['BBID', '12345', '67890']

    Returns
    -------
    str
        Regex alternation string with full anchors, e.g. '^1$|^100$|^200$'
    """
    return '|'.join('^' + b + '$' for b in bparts)


def get_dss_data(file, path_pattern):
    """
    returns a data frame of all the time series that match the path_pattern

    Parameters
    ----------
    file : str
        dss filename
    path_pattern : str
        path pattern of /A/B/C//E/F/ parts, e.g. ///FLOW//// will match any C part of FLOW 

    Returns
    -------
    DataFrame
        pandas DataFrame with columns of each data matched
    """
    data = dss.get_matching_ts(file, path_pattern)
    df = pd.concat([r[0] for r in data], axis=1, join='inner')
    return df


def sum(dfflows):
    ''' sum over the columns '''
    return dfflows.sum(axis=1)


def get_dcd_flows(file, bpart_pattern='[0-9]+|BBID', seepage=True, epart='1DAY',
                  div_cpart='DIV-FLOW', drain_cpart='DRAIN-FLOW', seep_cpart='SEEP-FLOW'):
    """
    gets the diversion, drainage and seepage flows as three data frames

    Parameters
    ----------
    file : str
        DSS filename for DCD or SMCD output
    bpart_pattern : str, optional
        pattern to match for BPART of file, by default '[0-9]+|BBID'
    seepage : bool, optional
        read seepage if True, by default True
    epart : str, optional
        DSS time interval E-part, by default '1DAY'
    div_cpart : str, optional
        C-part for diversion flows, by default 'DIV-FLOW'
    drain_cpart : str, optional
        C-part for drainage flows, by default 'DRAIN-FLOW'
    seep_cpart : str, optional
        C-part for seepage flows, by default 'SEEP-FLOW'

    Returns
    -------
    tuple
        returns a tuple of diversion flows, drainage flows and seepage flows for all nodes
    """
    div_flows = get_dss_data(file, f'//({bpart_pattern})/{div_cpart}//{epart}//')
    drain_flows = get_dss_data(file, f'//({bpart_pattern})/{drain_cpart}//{epart}//')
    if seepage:
        seep_flows = get_dss_data(file, f'//({bpart_pattern})/{seep_cpart}//{epart}//')
    if seepage:
        return div_flows, drain_flows, seep_flows
    else:
        return div_flows, drain_flows


def calculate_netcd(div_flows, drain_flows, seep_flows=None):
    '''
    calculate the total net channel depletions for all the div_flows, drain_flows and seep_flows
    netcd = div_flows - drain_flows +seep_flows

    if seep_flows is None then its calculated as div_flows - drain_flows
    '''
    if seep_flows is None:
        return sum(div_flows) - sum(drain_flows)
    else:
        return sum(div_flows) + sum(seep_flows) - sum(drain_flows)


@click.command(name="calc-netcd")
@click.argument("dssfile", type=click.Path(exists=True))
@click.option(
    "--bpart",
    multiple=True,
    help="B-part to include (repeatable, e.g. --bpart BBID --bpart 12345)",
)
@click.option(
    "--bpart-file",
    type=click.Path(exists=True),
    default=None,
    help="Text file with one B-part per line",
)
@click.option(
    "--no-seepage",
    is_flag=True,
    default=False,
    help="Exclude seepage flows (NetCD = DIV - DRAIN only)",
)
@click.option(
    "--epart",
    default="1DAY",
    show_default=True,
    help="Time interval E-part (e.g. 1DAY, 1MON)",
)
@click.option(
    "--div-cpart",
    default="DIV-FLOW",
    show_default=True,
    help="DSS C-part for diversion flows",
)
@click.option(
    "--drain-cpart",
    default="DRAIN-FLOW",
    show_default=True,
    help="DSS C-part for drainage flows",
)
@click.option(
    "--seep-cpart",
    default="SEEP-FLOW",
    show_default=True,
    help="DSS C-part for seepage flows",
)
@click.option(
    "--components",
    is_flag=True,
    default=False,
    help="Also write summed component columns (SUM_DIV, SUM_DRAIN, SUM_SEEP) alongside NETCD",
)
@click.option(
    "-o",
    "--output",
    default="netcd.csv",
    show_default=True,
    help="Output CSV file path",
)
def calc_netcd_cmd(dssfile, bpart, bpart_file, no_seepage, epart, div_cpart, drain_cpart, seep_cpart, components, output):
    """Calculate aggregated Net Channel Depletion (NetCD) from a DSS file.

    NetCD = DIV-FLOW - DRAIN-FLOW + SEEP-FLOW, summed over all matching B-parts.

    B-parts can be supplied via --bpart (repeatable) and/or --bpart-file (one per line).
    If neither is given, the default pattern matches all numeric node IDs and BBID.
    """
    bparts = list(bpart)
    if bpart_file:
        with open(bpart_file) as f:
            bparts.extend(line.strip() for line in f if line.strip())

    if bparts:
        bpart_pattern = bparts_to_pattern(bparts)
    else:
        bpart_pattern = get_bpart_pattern()

    try:
        div_flows = get_dss_data(dssfile, f"//({bpart_pattern})/{div_cpart}//{epart}//")
    except Exception as e:
        raise click.ClickException(f"No {div_cpart} data found for the specified B-parts: {e}")

    try:
        drain_flows = get_dss_data(dssfile, f"//({bpart_pattern})/{drain_cpart}//{epart}//")
    except Exception as e:
        raise click.ClickException(f"No {drain_cpart} data found for the specified B-parts: {e}")

    seep_flows = None
    if not no_seepage:
        try:
            seep_flows = get_dss_data(dssfile, f"//({bpart_pattern})/{seep_cpart}//{epart}//")
        except Exception:
            click.echo(
                f"Warning: No {seep_cpart} data found for the specified B-parts. "
                "Proceeding without seepage (NetCD = DIV - DRAIN).",
                err=True,
            )

    netcd = calculate_netcd(div_flows, drain_flows, seep_flows)
    meta = {
        "command": " ".join(sys.argv),
        "dssfile": dssfile,
        "bparts": ", ".join(bparts) if bparts else "default (all numeric + BBID)",
        "epart": epart,
        "div_cpart": div_cpart,
        "drain_cpart": drain_cpart,
        "seep_cpart": seep_cpart,
        "seepage": "excluded" if no_seepage else ("included" if seep_flows is not None else "not found (excluded)"),
    }
    if components:
        output_df = pd.DataFrame({"SUM_DIV": sum(div_flows), "SUM_DRAIN": sum(drain_flows)})
        if seep_flows is not None:
            output_df["SUM_SEEP"] = sum(seep_flows)
        output_df["NETCD"] = netcd
        write_csv_with_meta(output, output_df, meta)
    else:
        write_csv_with_meta(output, netcd, meta, header=["NETCD"])
    click.echo(f"NetCD written to {output}")
