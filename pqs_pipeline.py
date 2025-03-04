import datetime
import re
import pandas as pd
import requests
from sqlalchemy import create_engine
import psycopg2
from pipelineFunctions import *
from config import *
from db_conn import *
from crinicial_trails_pipeline import *
requests.packages.urllib3.disable_warnings()


def get_pqs_protocol_list_response(session, conf_file):
    responseArr = []

    active_protocol_url = conf_file['PQS_API']['pqs_active_protocol_url']
    # print(active_protocol_url)
    response = session.get(active_protocol_url, verify=False)
    responseArr.append(response)

    pqs_protocol_by_fical_year_url = conf_file['PQS_API']['pqs_protocol_by_fical_year_url']
    current_year = datetime.datetime.now().year
    
    for i in range(5):
        responseArr.append( session.get(pqs_protocol_by_fical_year_url + str(current_year - i), verify=False))   
    
    return responseArr;

def get_pqs_protocol_detail_response(session, conf_file, protocol_number):
    protocol_detail_url = conf_file['PQS_API']['pqs_protocol_detail_url'] + protocol_number
    # print(protocol_detail_url)
    response = session.get(protocol_detail_url, verify=False)
    return response;

def get_pqs_protocol_list(conf_file, conn):
    protocol_list_session = requests.Session()
    protocol_list_response_arr = get_pqs_protocol_list_response(protocol_list_session, conf_file)  

    protocol_list = set()
    for protocol_list_response in protocol_list_response_arr:
        study_json = protocol_list_response.json()['protocols'] 
        for study in study_json:
            protocol_list.add(study['protocolNumber'])
    
    # for p in protocol_list:
    #     if (p == '17-C-N141'):
    #         print ("protocol number: " + p)
    
    print ("protocol list size: " + str(len(protocol_list)))
    return protocol_list;

ct_data_fields_key = ['nctId', 'orgStudyIdInfo', 'title', 'clinicalTrialLink', 
                    'firstSubmitDate', 'primaryCompletionDate', 'lastUpdateSubmitDate']

def generate_pqs_tables(conf_file, conn):
    print ("PQS Pipeline Start Time: " + str(datetime.datetime.now()))
    protocol_list = get_pqs_protocol_list(conf_file, conn)

    protocol_detail_session = requests.Session()
    #protocol_detail_response = get_pqs_protocol_detail_response(protocol_detail_session, conf_file)
    
    protocol_data_fields = ['protocol_number', 'simpleProtocolNumber', 'protocol_title', 'accrual_inst','accrual_status','coord_site','protrak_accrual_status','research_type','research_phase', 'study_type',
               'start_date_of_study', 'primary_completion_date', 'date_first_part_enrolled', 'irb_name', 'z_number', 'nct_number', 'currentEnrollment','plannedEnrollment']
    
    pi_data_fields = ['protocol_number', 'simplifiedProtocolNumber', 'firstName', 'lastName', 'middleName', 'piName']


    detail_data = []
    pi_data = []
    ct_dataList = list()
    
    count = 0
    for protocol in protocol_list:   
        count += 1

        protocol_number = protocol
        simplified_protocol_number = protocol_number.replace('-', '')

        # if count > 100:
        #     break
        #calling api for specific protocol details
        protocol_detail_response = get_pqs_protocol_detail_response(protocol_detail_session, conf_file, protocol_number)
        if protocol_detail_response.json()['responseCode'] != 200:
            continue
        
        detail_json = protocol_detail_response.json()['returnedProtocol']
        
        #flattened_detail_table = pd.json_normalize(json_flatten(detail_json))
        # flattened_detail_json = json_flatten(detail_json)
        flattened_detail_json = dict.fromkeys(protocol_data_fields, None)
        flattened_detail_json['protocol_number'] = protocol_number
        flattened_detail_json['simpleProtocolNumber'] = simplified_protocol_number
        flattened_detail_json['protocol_title'] = detail_json.get('protocol_title', '')
        flattened_detail_json['accrual_inst'] = detail_json.get('accrual_inst', '')
        flattened_detail_json['accrual_status'] = detail_json.get('accrual_status', '')
        flattened_detail_json['coord_site'] = detail_json.get('coord_site_name', '')
        flattened_detail_json['protrak_accrual_status'] = detail_json.get('protrak_accrual_status', '')
        flattened_detail_json['research_type'] = detail_json.get('research_type', '')
        flattened_detail_json['research_phase'] = detail_json.get('research_phase', '')   
        flattened_detail_json['study_type'] = detail_json.get('study_type', '')
        flattened_detail_json['start_date_of_study'] = detail_json.get('start_date_of_study', '')
        flattened_detail_json['primary_completion_date'] = detail_json.get('primary_completion_date', '')
        flattened_detail_json['date_first_part_enrolled'] = detail_json.get('date_first_part_enrolled', '')
        flattened_detail_json['irb_name'] = detail_json.get('irb_name', '')
        flattened_detail_json['z_number'] = detail_json.get('z_number', '')
        flattened_detail_json['nct_number'] = detail_json.get('nct_number', '')

        #getting pi information from API call
        investigators = detail_json['investigators']
        for i in investigators:
            if i['r'] == 'PI':
                pi_detail_json = dict.fromkeys(pi_data_fields, None)
                pi_detail_json['protocol_number'] = protocol_number
                pi_detail_json['simplifiedProtocolNumber'] = simplified_protocol_number
                pi_detail_json['firstName'] = i['n']['fn']
                pi_detail_json['lastName'] = i['n']['ln']
                pi_detail_json['middleName'] = i['n']['mn']
                pi_detail_json['piName'] = '{0} {1} {2}'.format(i['n']['fn'],i['n']['mn'],i['n']['ln'])

                pi_data.append(pi_detail_json) if pi_detail_json not in pi_data else None

        #summarizing enrollment data
        enrollmentForms = detail_json.get('enrollment_forms','')
        if enrollmentForms:
            enrollmentForms = enrollmentForms[0]
            currentEnrollment = enrollmentForms['total_american_indian'] + enrollmentForms['total_asian'] + enrollmentForms['total_black'] + enrollmentForms['total_hawaiian'] + enrollmentForms['total_more_than_one_race'] + enrollmentForms['total_unknowns'] + enrollmentForms['total_white']
        else:
            currentEnrollment = ''

        targetEnrollments = detail_json.get('target_enrollments','')
        if targetEnrollments:
            targetEnrollments = targetEnrollments[0]
            targetEnrollment = targetEnrollments['total_american_indian'] + targetEnrollments['total_asian'] + targetEnrollments['total_black'] + targetEnrollments['total_hawaiian'] + targetEnrollments['total_more_than_one_race'] + targetEnrollments['total_white']
        else:
            targetEnrollment = ''

        flattened_detail_json['currentEnrollment'] = currentEnrollment
        flattened_detail_json['plannedEnrollment'] = targetEnrollment

        #flattening coord_site data
        flattened_detail_json['coord_site'] = detail_json.get('coord_site_name','')
        flattened_detail_json['primaryCompletionDate'] = detail_json.get('primary_completion_date')

        global ct_data_fields_key
        nct_number = detail_json.get('nct_number','')
        if (nct_number and nct_number != "N/A"):
            # print("nct_number: ", nct_number)
            ct_dataList = generate_ct_tables(conf_file, nct_number, ct_dataList, ct_data_fields_key)

        #detail_table = pd.concat([detail_table, flattened_detail_table])
        detail_data.append(flattened_detail_json) if flattened_detail_json not in detail_data else None

    pi_table = pd.json_normalize(pi_data)
    detail_table = pd.json_normalize(detail_data)

    final_table = detail_table[protocol_data_fields].merge(pi_table, on='protocol_number')
    final_table.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0))
    final_table.index += 1
    final_table.to_sql('Protrak_Data_Table', con=conn, if_exists='replace', index=True, index_label='id', method='multi')

    
    pi_table.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0))
    pi_table.index += 1
    pi_table.to_sql('Protrak_PI_Table', con=conn, if_exists='replace', index=True, index_label='id', method='multi')

    ct_protocol_table = pd.json_normalize(ct_dataList)
    ct_protocol_table.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0))
    ct_protocol_table.index += 1
    ct_protocol_table.to_sql('CT_Data_Table', con=conn, if_exists='replace', index=True, index_label='id', )
    
    print ("PQS Pipeline End Time: " + str(datetime.datetime.now()))


if __name__ == '__main__': 
    conf_file = load_config_file()
      
    conn = get_db_connection(conf_file)

    # protocol_list = get_pqs_protocol_list(conf_file, conn)  


    generate_pqs_tables(conf_file, conn)

    print('PQS and CT Pipeline Completed')