#!/usr/bin/env python
import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
from config import *
from db_conn import *
requests.packages.urllib3.disable_warnings()

def generate_nidcr_redcap_tables(conf_file, conn):
    print ("NIDCR REDCap Pipeline Start Time: " + str(datetime.datetime.now()))
    data = {
        'content': 'record',
        'action': 'export',
        'format': 'json',
        'type': 'flat',
        'csvDelimiter': '',
        'records[0]': '00-D-0183',
        'records[1]': '01-D-0184',
        'records[2]': '04-D-0267',
        'records[3]': '05-D-0050',
        'records[4]': '05-D-0051',
        'records[5]': '05-D-0058',
        'records[6]': '05-D-0103',
        'records[7]': '06-D-0188',
        'records[8]': '06-D-0206',
        'records[9]': '07-D-0016',
        'records[10]': '07-D-N044',
        'records[11]': '10-D-0020',
        'records[12]': '10-D-0180',
        'records[13]': '11-D-0094',
        'records[14]': '11-D-0172',
        'records[15]': '12-D-0100',
        'records[16]': '12-H-0078',
        'records[17]': '13-D-0014',
        'records[18]': '13-D-0025',
        'records[19]': '13-D-0033',
        'records[20]': '14-D-0145',
        'records[21]': '15-D-0051',
        'records[22]': '15-D-0097',
        'records[23]': '15-D-0129',
        'records[24]': '16-D-0040',
        'records[25]': '16-M-0093',
        'records[26]': '18-D-0007',
        'records[27]': '18-D-0086',
        'records[28]': '18-D-0114',
        'records[29]': '18-D-0121',
        'records[30]': '20-D-0094',
        'records[31]': '20-D-0122',
        'records[32]': '20-D-0131',
        'records[33]': '84-D-0056',
        'records[34]': '94-D-0188',
        'records[35]': '98-D-0116',
        'records[36]': '98-D-0145',
        'records[37]': '98-D-0146',
        'records[38]': '99-D-0070',
        'records[39]': '000123',
        'records[40]': '000350',
        'records[41]': '000780',
        'records[42]': '000798',
        'records[43]': '000881',
        'records[44]': '001085',
        'records[45]': '001089',
        'records[46]': '001193',
        'records[47]': '001521',
        'records[48]': '001833',
        'records[49]': '001939',
        'records[50]': '002290',
        'fields[0]': 'pi',
        'fields[1]': 'current_enrollment',
        'fields[2]': 'study_risk_complexity',
        'fields[3]': 'upcoming_dsmc_date',
        'fields[4]': 'last_dsmc_review_date',
        'fields[5]': 'next_imv',
        'fields[6]': 'last_imv_date',
        'fields[7]': 'next_iqc',
        'fields[8]': 'database',
        'fields[9]': 'crada_comments',
        'fields[10]': 'mta_comments',
        'fields[11]': 'study_oversight',
        'fields[12]': 'method_add_oversight',
        'fields[13]': 'protocol_number',
        'fields[14]': 'planned_enroll_num',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    data['token'] = conf_file['REDCAP_API']['nidcr_redcap_token']
    api_url = conf_file['REDCAP_API']['nidcr_redcap_url']
    r = requests.post(api_url,data=data, verify=False)
    print('HTTP Status: ' + str(r.status_code))

    table = pd.json_normalize(r.json())
    table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))
    table.index += 1
    table.to_sql('NIDCR_REDCap_Table', con=conn, if_exists='replace', index=True, index_label='id')

    print ("NIDCR REDCap Pipeline End Time: " + str(datetime.datetime.now()))

if __name__ == '__main__': 
    conf_file = load_config_file()
      
    conn = get_db_connection(conf_file)

    generate_nidcr_redcap_tables(conf_file, conn)

    print('NIDCR_REDCap Pipeline Completed')
