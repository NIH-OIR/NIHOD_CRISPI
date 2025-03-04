import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from config import *
from db_conn import *

requests.packages.urllib3.disable_warnings()

def get_ct_response(session, conf_file, nct_number):
    ct_api = conf_file['CT_GOV_API']['ctgov_study_api_url'] + nct_number
    # print(ct_api)
    response = session.get(ct_api, verify=False)
    return response;

def generate_ct_tables(conf_file, nct_number, ct_dataList, ct_data_fields_key):
    session = requests.Session()
    ct_response = get_ct_response(session, conf_file, nct_number)

    ct_data_fields = dict.fromkeys(ct_data_fields_key, None)

    if ct_response.status_code != 200:
        return ct_dataList

    study = ct_response.json()
    data = study.get('protocolSection', '')
    # print("data: ", data)

    identificationData = data.get('identificationModule', '')
    nctNumber = identificationData.get('nctId')
    ct_data_fields['nctId'] = nctNumber
    orgStudyIdInfo = identificationData.get('orgStudyIdInfo', '')
    if orgStudyIdInfo:
        orgStudyIdInfo = orgStudyIdInfo.get('id','')
    ct_data_fields['orgStudyIdInfo'] = orgStudyIdInfo
    #secondaryId = identificationData.get('secondaryIdInfos')
    title = identificationData.get('officialTitle', '')
    ct_data_fields['title'] = title

    studyURL = conf_file['CT_GOV_API']['ctgov_study_web_url'] + nctNumber
    ct_data_fields['clinicalTrialLink'] = studyURL

    statusData = data.get('statusModule', '')
    firstSubmitDate = statusData.get('studyFirstSubmitDate')
    ct_data_fields['firstSubmitDate'] = firstSubmitDate
    primaryCompletionDate = statusData.get('primaryCompletionDateStruct','')
    if primaryCompletionDate:
        primaryCompletionDate = primaryCompletionDate['date']
    ct_data_fields['primaryCompletionDate'] = primaryCompletionDate
    lastUpdateSubmitDate = statusData.get('lastUpdateSubmitDate','')
    ct_data_fields['lastUpdateSubmitDate'] = lastUpdateSubmitDate

    ct_dataList.append(ct_data_fields)


    return ct_dataList;
    # ct_protocol_table = pd.json_normalize(dataList)

    # ct_protocol_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))

    # ct_protocol_table.to_sql('CT_Data_Table', con=conn, if_exists='replace')
