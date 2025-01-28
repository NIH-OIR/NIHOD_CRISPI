
import yaml
from sqlalchemy import create_engine
from pipelineFunctions import *
from pqs_pipeline import *
from protect_irb_submission_pipeline import *
from NIDB_pipeline import *
from NIDCR_REDCap_pipeline import *
from config import *
from db_conn import *

if __name__ == '__main__': 
    
    conf_file = load_config_file()
    conn = get_db_connection(conf_file)

    #pqs pipeline including CT
    generate_pqs_tables(conf_file, conn)
    #protect irb pipeline
    generate_protect_irb_tables(conf_file, conn)
    #redcap pipeline
    generate_nidcr_redcap_tables(conf_file, conn)
    #NIDB pipeline
    generate_NIDB_tables(conf_file, conn)