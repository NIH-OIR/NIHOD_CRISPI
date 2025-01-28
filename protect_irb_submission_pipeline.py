import re
import pandas as pd
import requests
import json
import yaml
import sqlalchemy
import datetime
from sqlalchemy import create_engine
from config import *
from db_conn import *

requests.packages.urllib3.disable_warnings()
def authorize_api_call(session, conf_file):
    username = conf_file['IRB_CONNECTION']['username']
    psw = conf_file['IRB_CONNECTION']['key']
    #print("username: " + username +" key: " + psw)
    session.auth = (username, psw)
    return session

def get_irb_submission_list_response(session, conf_file):
    session = authorize_api_call(session, conf_file)
    irb_protocols_url = conf_file['IRB_API']['irb_submission_list_url']
    # print(irb_protocols_url)    
    response = session.get(irb_protocols_url, verify=False)
    return response;

def get_protect_study_detail_response(session, conf_file, submission_id):
    session = authorize_api_call(session, conf_file)
    irb_study_detail_url = conf_file['IRB_API']['irb_submission_details_url'] + submission_id
    # print(irb_study_detail_url)
    response = session.get(irb_study_detail_url, verify=False)
    return response;
def generate_protect_irb_tables(conf_file, conn):
    print ("IRB Submission Start Time: " + str(datetime.datetime.now()))

    session = requests.Session()
    irb_submission_list_response = get_irb_submission_list_response(session,conf_file)
    print ("irb submission list size: " + str(len(irb_submission_list_response.json())))
    
    study_id_list = set()
    submission_list = list()

    for j in irb_submission_list_response.json():
        submission_list.append(j['ID'])
        if j['submissionType'] == 'Initial Study':
            study_id_list.add(j['parentStudyID'])
    
    print ("irb initial study list size: " + str(len(study_id_list)))
    
    protect_data_fields = ['parentStudyID', 'simplifiedParentStudyID', 'piFirstName', 'piMiddleName', 'piLastName', 'piEmail', 'title', 'shortTitle', 'riskLevel', 'dateApproved', 'dateExpiration', #'studyTeamMembers',
                       'reportableNewInformation_category', 'reportableNewInformation_relatedStudies', 'reportableNewInformation_supportingDocuments', 'reportableNewInformation_rniDetermination']
    
    study_details_list = list()



    for id in list(study_id_list):

        response2 = get_protect_study_detail_response(session, conf_file, id)
        if irb_submission_list_response.status_code != 200:
            continue

        if 'data' in response2.json().keys():
            data = response2.json()['data']
        else:
            data = response2.json()

        simplifiedParentStudyID = ''.join(re.findall('[0-9]+', data['parentStudyID']))
        data['simplifiedParentStudyID'] = simplifiedParentStudyID
        
        #protocols_done.append(id)
        #study_details_list.append(data)
        study_details_list.append(data)
    
    protect_protocol_table = pd.json_normalize(study_details_list)[protect_data_fields]
    protect_protocol_table['reportableNewInformation_category'] = protect_protocol_table['reportableNewInformation_category'].apply(json.dumps)
    protect_protocol_table['reportableNewInformation_relatedStudies'] = protect_protocol_table['reportableNewInformation_relatedStudies'].apply(json.dumps)
    protect_protocol_table['reportableNewInformation_rniDetermination'] = protect_protocol_table['reportableNewInformation_rniDetermination'].apply(json.dumps)
    protect_protocol_table['reportableNewInformation_supportingDocuments'] = protect_protocol_table['reportableNewInformation_supportingDocuments'].apply(json.dumps)

    protect_protocol_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))
    protect_protocol_table.index += 1
    protect_protocol_table.to_sql('Protect_Data_Table', con=conn, if_exists='replace', dtype={'reportableNewInformation_category':sqlalchemy.types.JSON}, index=True, index_label='id')

    print ("IRB Submission Completed Time: " + str(datetime.datetime.now()))

if __name__ == '__main__': 
    conf_file = load_config_file()
      
    conn = get_db_connection(conf_file)

    generate_protect_irb_tables(conf_file, conn)

    print('Protect IRB Pipeline Completed')