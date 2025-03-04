#!/bin/python3
import os
from sqlalchemy import *


def create_views_from_sqls(engine):
    folder_path = 'sql_views'
    with engine.connect() as connection:
        for filename in os.listdir(folder_path):
            if filename.endswith(".sql"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, "r") as sql_file:
                    sql_script = sql_file.read()
                    for statement in sql_script.split(';'): #Splitting by semicolon to handle multiple statements
                        if statement.strip(): #Ensuring statement is not empty
                            connection.execute(text(statement))
        connection.commit()