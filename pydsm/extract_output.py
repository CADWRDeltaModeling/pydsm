import pyhecdss
from vtools.functions.filter import cosine_lanczos, godin_filter
def extract_from_dss(dssfile,loc,type='EC',filter='lanczos'):
    df,units,ptype=pyhecdss.get_rts(dssfile,'//%s/EC////'%loc)[0]
    if filter == 'lanczos':
        fdf = cosine_lanczos(df)
    elif filter == 'godin':
        fdf = godin_filter(df)
    else:
        fdf = cosine_lanczos(df)
    return df,fdf
