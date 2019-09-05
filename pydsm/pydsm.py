import click
import pyhecdss
from pydsm import filter

@click.command()
@click.option("--dssfile", help="path to DSS File to be processed")
@click.option("--outfile", help="path to output file (ends in .h5)")
def godin_filter(dssfile, outfile):
    d=pyhecdss.DSSFile(dssfile)
    catdf=d.read_catalog()
    catec=catdf[catdf.C=='EC']
    plist=d.get_pathnames(catec)
    r,u,p=d.read_rts(plist[0])
    #import pdb; pdb.set_trace()
    results=filter.godin_filter(r)
    for p in plist[1:]:
        r,u,p=d.read_rts(p)
        rtg=filter.godin_filter(r)
        results=results.join(rtg,how='outer')
    results.to_hdf(outfile,'godin_ec')
#
"""Main module."""
if __name__ == '__main__':
    godin_filter()
#
