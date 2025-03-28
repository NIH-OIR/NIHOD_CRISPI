#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import pandas as pd
import requests
import json
import yaml
import sqlalchemy
from sqlalchemy import create_engine
from config import *
from db_conn import *

requests.packages.urllib3.disable_warnings()


# In[42]:


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

def get_protect_scientific_review_response(session, conf_file, submission_id):
    session = authorize_api_call(session, conf_file)
    irb_scientific_review_url = conf_file['SCIENTIFIC_REVIEW_API']['scientific_review_list_url'] + "'" + submission_id + "'"
    print(irb_scientific_review_url)
    response = session.get(irb_scientific_review_url, verify=False)
    return response;

def get_protect_scientific_review_detail_response(session, conf_file, submission_id):
    session = authorize_api_call(session, conf_file)
    irb_scientific_review_url = conf_file['SCIENTIFIC_REVIEW_API']['scientific_review_details_url'] + "'" + submission_id + "'"
    print(irb_scientific_review_url)
    response = session.get(irb_scientific_review_url, verify=False)
    return response;


# In[ ]:


def generate_protect_irb_tables(conf_file, conn):
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
    
    sr_data_fields = ['ID', 'parentStudyID', 'state', 'srStudyType', 'piFirstName', 'piMiddleName', 'piLastName', 'title', 'submissionTypeID', 
                      'submissionType', 'dateApproved', 'annualDueDate', 
                       'quadrennialDueDate', 'srModifiedDate', 'srcWorkSpaceLink']
    sr_detail_fields = ['submissionType', 'SRID', 'SRName', 'SRParentStudy', 'followUpDueDate', 'qRDueDate', 'srSubmittedDate', 'srExpeditedReviewSubmittedDate', 'srMeetingDate', 'dateApproved', 'determination'] 
                      
    sr_details_list = list()
    sr_details = list()
    

    k=0 #only do 5 for now
    for id in list(study_id_list):
            print("id: " + id + " k: " + str(k))
            print()
            response2 = get_protect_scientific_review_response(session, conf_file, id)
            print(response2.json())
            k+=1
            if response2.status_code != 200:
                continue
            
            if 'data' in response2.json():
                data = response2.json()['data']
            else:
                data = response2.json()

            print("data======="+str(data))
            for l in data:
                print("l: " + str(l))
                if l['state'] == 'Discarded':
                    continue
            
                sr_details_list.append(l)    

                srId = l['ID']
                if srId != '':
                    response3 = get_protect_scientific_review_detail_response(session, conf_file, srId)  
                    print(response3.json()) 
                    if response3.status_code != 200:
                        continue
            
                    if 'data' in response3.json():
                        data1 = response3.json()['data']
                    else:
                        data1 = response3.json()
                    
                    print("data1--------"+str(data1))
                    sr_details.append(data1)                        
    
    sr_protocol_table = pd.json_normalize(sr_details_list)
    sr_protocol_table.to_sql('SR_Data_Table', con=conn, if_exists='replace', index='false')
    sr_detail_table = pd.json_normalize(sr_details)[sr_detail_fields]
    
    sr_detail_table.to_sql('SR_Details_Table', con=conn, if_exists='replace', index='false')


    
    print(irb_submission_list_response.status_code)   
    


# In[ ]:


if __name__ == '__main__': 
    conf_file = load_config_file()
      
    conn = get_db_connection(conf_file)

    generate_protect_irb_tables(conf_file, conn)
    #generate_protect_irb_tables(conf_file)

