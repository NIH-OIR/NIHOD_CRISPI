from sqlalchemy import create_engine

def get_db_connection(conf_file):
    dbConnectionString = conf_file['DB_SRC']['connection_string']
    # print(dbConnectionString)

    db = create_engine(dbConnectionString)
    conn = db.connect()
    return conn

def get_db_engine(conf_file):
    dbConnectionString = conf_file['DB_SRC']['connection_string']
    # print(dbConnectionString)

    db = create_engine(dbConnectionString)
    return db