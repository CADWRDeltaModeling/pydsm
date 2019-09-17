import sys
import click
import pyhecdss
from pydsm import filter


@click.group()
def main():
    pass


def _build_column(columns, cpart_append, epart_replace=None):
    '''
    builds column name based on /A/B/C/D/E/F/ DSS pathname and 
    replacing the cpart with existing cpart + cpart_append value
    '''
    def append_cpart(name):
        parts=name.split('/')
        parts[3]=parts[3]+cpart_append
        if epart_replace:
            parts[5]=epart_replace
        return '/'.join(parts)
    return [ append_cpart(name) for name in columns ]

def _extract_processing(df, godin_filter, daily_average, daily_max, daily_min, monthly_average):
    results = df
    if godin_filter:
        results = filter.godin_filter(results)
        print(results.columns)
    if daily_average: # if godin filtered then replace that with daily averaged values
        tdf = results.resample('1D').mean()
        tdf.columns = _build_column(df.columns,'-DAILY-AVG','1DAY')
        print(tdf.columns)
        results=tdf
    if daily_max:
        tdf = df.resample('1D').max()
        tdf.columns = _build_column(df.columns,'-DAILY-MAX','1DAY')
        print(tdf.columns)
        results=results.join(tdf, how='outer')
        print(results.columns)
    if daily_min:
        tdf = df.resample('1D').min()
        tdf.columns = _build_column(df.columns,'-DAILY-MIN','1DAY')
        results=results.join(tdf, how='outer')
    if monthly_average:
            results_monthly = df.resample('M').mean()
            results_monthly.columns = _build_column(df.columns,'-MONTHLY-AVG','1MON')
    return results, results_monthly

@click.command()
@click.option("-o", "--outfile", default="out.gz", help="path to output file (ends in .zip, .gz, .bz2 for compression)")
@click.option("--cpart", help="filter by cpart string match (e.g. EC for only loading EC)")
@click.option("-godin","--godin-filter", is_flag=True, default=False, help="apply godin filter before writing out")
@click.option("-davg","--daily-average", is_flag=True, default=False, help="average to daily values")
@click.option("-dmax","--daily-max", is_flag=True, default=False, help="maximum daily value")
@click.option("-dmin","--daily-min", is_flag=True, default=False, help="minimum daily value")
@click.option("-mavg","--monthly-average", is_flag=True, default=False, help="monthly average value")
@click.argument("dssfile", type=click.Path(exists=True))
def extract_dss(dssfile, outfile, cpart, godin_filter, daily_average, daily_max, daily_min, monthly_average):
    '''
    Extract data from DSS file, optionally filtering it and writing to a pickle for quick future loads
    '''
    pyhecdss.set_message_level(0)
    d = pyhecdss.DSSFile(dssfile)
    catdf = d.read_catalog()
    catec = catdf[catdf.C == cpart]
    plist = d.get_pathnames(catec)
    if len(plist) == 0:
        print("No pathnames found in dssfile: %s for cpart=%s" %
              (dssfile, cpart))
    print('Processing: %s'%plist[0])
    r, u, p = d.read_rts(plist[0])
    results_daily, results_monthly = _extract_processing(
        r, godin_filter, daily_average, daily_max, daily_min, monthly_average)
    for p in plist[1:]:
        print('Processing: %s'%p)
        r, u, p = d.read_rts(p)
        rtg = _extract_processing(
            r, godin_filter, daily_average, daily_max, daily_min, monthly_average)
        results_daily = results_daily.join(rtg, how='outer')
        results_monthly = results_monthly.join(rtg, how='outer')
    results_daily.to_pickle(outfile)
    results_monthly.to_pickle(outfile)


# adding sub commands to main
main.add_command(extract_dss)

if __name__ == "__main__":
    sys.exit(main())
