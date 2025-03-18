import pandas as pd
import requests
from config import *
from sqlalchemy import *
from db_conn import *
from db_util import *
import datetime
import logging

requests.packages.urllib3.disable_warnings()

def get_ct_response(session, conf_file, nct_number):
    ct_api = conf_file['CT_GOV_API']['ctgov_study_api_url'] + nct_number
    # print(ct_api)
    response = session.get(ct_api, verify=False)
    return response;

def generate_ct_dataList(conf_file, nct_number, ct_dataList, ct_data_fields_key):
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
    ct_data_fields['resultsFirstSubmitDate'] = statusData.get('resultsFirstSubmitDate','')

    ct_dataList.append(ct_data_fields)

    return ct_dataList;
    # ct_protocol_table = pd.json_normalize(dataList)

    # ct_protocol_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))

    # ct_protocol_table.to_sql('CT_Data_Table', con=conn, if_exists='replace')

def get_NctId_from_DB(engine):
    raw_conn = engine.raw_connection()
    cursor = raw_conn.cursor()
    cursor.execute('SELECT DISTINCT nct_number FROM "Protrak_Data_Table"')
    nctIds = cursor.fetchall()
    return nctIds

def generate_ct_tables(conf_file, engine):
    logger = logging.getLogger(__name__)
    
    print ("CT Pipeline Start Time: " + str(datetime.datetime.now()))
    logger.info("CT Pipeline Start Time: " + str(datetime.datetime.now()))
    
    nctIds = get_NctId_from_DB(engine)
    print("number of nctIds: ", len(nctIds))
    logger.info("number of nctIds: ", len(nctIds))
    
    ct_data_fields_key = ['nctId', 'orgStudyIdInfo', 'title', 'clinicalTrialLink', 
                    'firstSubmitDate', 'primaryCompletionDate', 'lastUpdateSubmitDate']
    ct_dataList = list()
    for nctId in nctIds:
        nct_number = str(nctId[0])
        # print("nct_number: ", nct_number)
        if (nct_number and nct_number != "N/A" and nct_number != "None"):
            ct_dataList = generate_ct_dataList(conf_file, nct_number, ct_dataList, ct_data_fields_key) 
    
    ct_protocol_table = pd.json_normalize(ct_dataList)
    ct_protocol_table.insert(0, 'TimeStamp', pd.to_datetime('now').replace(microsecond=0))
    ct_protocol_table.index += 1
        
    ct_table_name = 'CT_Data_Table'
    with engine.connect() as conn:
        try:
            query = text(f'DROP TABLE IF EXISTS "{ct_table_name}" CASCADE;')
            conn.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error dropping table: {e}")
            logger.error(f"Error dropping table: {e}")
            conn.rollback()

    ct_protocol_table.to_sql(ct_table_name, con=engine.connect(), if_exists='replace', index=True, index_label='id')
    print ("CT Pipeline End Time: " + str(datetime.datetime.now()))
    logger.info("CT Pipeline End Time: " + str(datetime.datetime.now()))



if __name__ == '__main__':
    logging.basicConfig(filename='pipelinelog.log')
    conf_file = load_config_file()
      
    engine = get_db_engine(conf_file)
    generate_ct_tables(conf_file, engine)
    create_views_from_sqls(engine)
    print('CT Pipeline Completed')
