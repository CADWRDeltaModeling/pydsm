import pyhecdss

def create_dsm2_input_for_cd(dss_filename, dsm2_input_filename, file_field_string):
    fout = open(dsm2_input_filename, 'w')
    with pyhecdss.DSSFile(dss_filename) as d:
        catdf = d.read_catalog()
        fout.write('# Consumptive Use input file for DSM2\n')
        fout.write('# This file was created automatically by pydsm.create_cd_inp.py\n')
        fout.write('SOURCE_FLOW\n')
        fout.write("%-15s %4s %5s %-7s %-18s %-s\n" % ('NAME','NODE','SIGN','FILLIN','FILE','PATH'))

        for row in catdf.iterrows():
            line=''
            name=''
            node = row[1]['B']
            sign=''
            fillin='last'
            if 'DIV' in row[1]['C']:
                name = 'dicu_div_' + node
                sign='-1'
            elif 'SEEP' in row[1]['C']:
                name ='dicu_seep_' + node
                sign='-1'
            elif 'DRAIN' in row[1]['C']:
                name ='dicu_drain_' + node
                sign='1'
            else:
                print('ERROR: C part does not contain DIV, DRAIN, or SEEP!. Catalog line='+row)
            s = '/'
            # line = name+'\t'+node+'\t'+sign+'\t'+fillin+'\t'+file_field_string+'\t'+s+row[1]['A']+s+row[1]['B']+s+row[1]['C']+s+s+row[1]['E']+s+row[1]['F'].strip()+s+'\n'
            # fout.write(line)
            fout.write("%-15s %4s %5s %-7s %-18s /%s/%s/%s/%s/%s/\n" % 
                       (name, node, sign, fillin, file_field_string, row[1]['A'], row[1]['B'], row[1]['C'], row[1]['E'], row[1]['F']))
        d.close()
    fout.write("END\n")
    fout.close()

######################
# example output file
######################
# #Delta Island Consumptive Use Estimated by Delta CD
# SOURCE_FLOW
# NAME           NODE SIGN FILLIN FILE        PATH
# dicu_div_1        1   -1 last   ${DICUFILE} /DELTACD-HIST+NODE/1/DIV-FLOW//1DAY/DWR-BDO/
# dicu_seep_1       1   -1 last   ${DICUFILE} /DELTACD-HIST+NODE/1/SEEP-FLOW//1DAY/DWR-BDO/
# dicu_drain_1      1    1 last   ${DICUFILE} /DELTACD-HIST+NODE/1/DRAIN-FLOW//1DAY/DWR-BDO/
# dicu_div_10      10   -1 last   ${DICUFILE} /DELTACD-HIST+NODE/10/DIV-FLOW//1DAY/DWR-BDO/
# dicu_seep_10     10   -1 last   ${DICUFILE} /DELTACD-HIST+NODE/10/SEEP-FLOW//1DAY/DWR-BDO/
# dicu_drain_10    10    1 last   ${DICUFILE} /DELTACD-HIST+NODE/10/DRAIN-FLOW//1DAY/DWR-BDO/
# dicu_div_100    100   -1 last   ${DICUFILE} /DELTACD-HIST+NODE/100/DIV-FLOW//1DAY/DWR-BDO/
# dicu_seep_100   100   -1 last   ${DICUFILE} /DELTACD-HIST+NODE/100/SEEP-FLOW//1DAY/DWR-BDO/
# dicu_drain_100  100    1 last   ${DICUFILE} /DELTACD-HIST+NODE/100/DRAIN-FLOW//1DAY/DWR-BDO/
def main():
    dss_filename = r'e:/full_calibration_8_3/delta/dsm2_studies/timeseries/dcd_dsm2_mss1.dss'
    dsm2_input_filename='e:/full_calibration_8_3/delta/dsm2_studies/timeseries/test.inp'
    file_field_string = '${DICUFILE}'
    create_dsm2_input_for_cd(dss_filename, dsm2_input_filename, file_field_string)

if __name__ == "__main__":
    main()