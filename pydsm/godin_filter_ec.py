import pyhecdss
from pydsm import filter
def godin_filter(dssfile):
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
    fname=dssfile.split('.')[0]
    results.to_hdf(fname+'_godin.h5','godin_ec')
if __name__ == '__main__':
    import sys
    print(sys.argv)
    godin_filter(sys.argv[1])
#
