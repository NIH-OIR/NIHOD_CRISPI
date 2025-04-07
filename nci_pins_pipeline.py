import datetime
import re
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
from pipelineFunctions import *
from config import *
from db_conn import *
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def authorize_api_call(session, conf_file):
    username = conf_file['PINS_CONNECTION']['username']
    psw = conf_file['PINS_CONNECTION']['password']
    # print("username: " + username +" password: " + psw)
    session.auth = (username, psw)
    return session

def get_pins_protocol_list(session, conf_file):
    session = authorize_api_call(session, conf_file)
    pins_protocol_list_url = conf_file['PINS_PROD_API']['pins_protocol_list_url']
    print("pins_protocol_list_url: " + pins_protocol_list_url) 
    response = session.get(pins_protocol_list_url, verify=False)      

    return response  

def generate_nci_pins_tables(conf_file, conn):
    session = requests.Session()

    protocol_list_response = get_pins_protocol_list(session, conf_file)
    if protocol_list_response.status_code != 200:
        print ("Error: " + str(protocol_list_response.status_code))
    else:
        #total 2210
        print ("PINS protocol list size: " + str(len(protocol_list_response.json())))

    pins_data_fields_key = ['number','formattedProtocolNumber', 'shortTitle', 'active', 'piFullName', 'piEmail', 'piNedId', 'accrualCeiling', 
                            'sponsorNumber', 'sponsor', 'multiInstitutional', 'nihCoordinatingSite', 'externalIrbInvolved', 'externalIrbName',
                            'protocolPhase', 'protocolStatus', 'protocolCategory', 'interventionalModel', 'masking', 'riskLevel', 
                            'twoStepEnrollment']
    protocol_details_list = list()
    pins_drug_list = list()
    pins_device_list = list()
    i = 0
    pins_data_fields = dict.fromkeys(pins_data_fields_key, None)
    for protocol_data in protocol_list_response.json():
        # print(protocol_data['formattedProtocolNumber']) 
        # for key in pins_data_fields_key:
        #     pins_data_fields[key] = protocol_data[key]
        protocol_details_list.append(protocol_data)

        #getting device and drug data
        drug_data = protocol_data['drugs']
        device_data = protocol_data['devices']

        if drug_data:
            for i in drug_data:
                i['protocolNumber'] = protocol_data['formattedProtocolNumber']
                i.pop('id')
                pins_drug_list.append(i)
        if device_data:    
            for i in device_data:
                i['protocolNumber'] = protocol_data['formattedProtocolNumber']
                i.pop('id')
                pins_device_list.append(i)

    pins_protocol_table = pd.json_normalize(protocol_details_list)[pins_data_fields_key]
    pins_protocol_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))
    pins_protocol_table.index += 1
    pins_protocol_table.to_sql('pins_protocol_table', con=conn, if_exists='replace', index=True, index_label='id')

    pins_device_table = pd.json_normalize(pins_device_list)
    pins_device_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))
    pins_device_table.index += 1
    pins_device_table.to_sql('pins_device_table', con=conn, if_exists='replace', index=True, index_label='id')

    pins_drug_table = pd.json_normalize(pins_drug_list)
    pins_drug_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))
    pins_drug_table.index += 1
    pins_drug_table.to_sql('pins_drug_table', con=conn, if_exists='replace', index=True, index_label='id')    

if __name__ == '__main__': 
    conf_file = load_config_file()
      
    conn = get_db_connection(conf_file)

    print ("NCI PINS Pipeline Start Time: " + str(datetime.datetime.now()))
    generate_nci_pins_tables(conf_file, conn)

    print('NCI PINS Pipeline Completed at: '+ str(datetime.datetime.now()))