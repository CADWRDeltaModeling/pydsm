EPART_TO_BLOCK_MAP = {'30MIN':'1DAY','12HOUR':'1MONTH','1DAY':'1YEAR','1MON':'1DECADE','1YEAR': '1CENTURY'}

def bridge_irreg_ts(dfits, epart):
    '''Bridge the last value from previous block to next blocks first time stamp'''
    epart = epart.split('IR-')[1]
    
