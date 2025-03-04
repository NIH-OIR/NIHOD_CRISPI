import datetime
import oracledb
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from config import *
from db_conn import *

def get_NIDB_connection(conf_file):
    dsn = conf_file['NIDB_SRC']['dsn']
    user = conf_file['NIDB_SRC']['user']
    password = conf_file['NIDB_SRC']['password']
    con = oracledb.connect(user=user,password=password,dsn=dsn)
    return con

def generate_NIDB_tables(conf_file, conn):
    print ("NIDB Pipeline Start Time: " + str(datetime.datetime.now()))
    nidb_conn = get_NIDB_connection(conf_file)
    cur = nidb_conn.cursor()
    
    cur.execute('SELECT * FROM NSD.CRISPI_PUBS')
    crispi_pubs_columns = cur.description
    crispi_pubs_data = cur.fetchall()

    crispi_pubs_combined = list()
    for entry in crispi_pubs_data:
        temp = dict()
        for a,b in zip(crispi_pubs_columns, entry):
            temp[a[0]] = b

        crispi_pubs_combined.append(temp)

    crispi_pubs_table = pd.DataFrame(crispi_pubs_combined)

    crispi_pubs_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))

    crispi_pubs_table.to_sql('NIDBCrispiPubs', con=conn, if_exists='replace', index=True, index_label='id')

    cur.execute('SELECT * FROM NSD.CRISPI_REPORTS')
    crispi_reports_columns = cur.description
    crispi_reports_data = cur.fetchall()

    crispi_reports_combined = list()
    for entry in crispi_reports_data:
        temp = dict()
        for a,b in zip(crispi_reports_columns, entry):
            temp[a[0]] = b
            if a[0] == 'RPID':
                b = b.replace(' ','').replace('-','')
                b = b[1:]
                temp['RPID_FORMATTED'] = b

        crispi_reports_combined.append(temp)

    crispi_reports_table = pd.DataFrame(crispi_reports_combined)

    crispi_reports_table.insert(0,'TimeStamp',pd.to_datetime('now').replace(microsecond=0))

    crispi_reports_table.to_sql('NIDBCrispiReports', con=conn, if_exists='replace', index=True, index_label='id')

    print ("NIDB Pipeline End Time: " + str(datetime.datetime.now()))

if __name__ == '__main__': 
    conf_file = load_config_file()
      
    conn = get_db_connection(conf_file)

    generate_NIDB_tables(conf_file, conn)

    print('NIDB Pipeline Completed')
