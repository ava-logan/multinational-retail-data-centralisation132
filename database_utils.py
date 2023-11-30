import yaml 
with open('db_creds.yaml', 'r') as file:
    data = yaml.safe_load(file)

import sqlalchemy as sa
import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    #used to connect and upload data to database

    def read_db_creds(self, data):
    #read creds in yaml and return dictionary 
        creds_dict = {}
        for key, value in data:
            creds_dict[key]=value
        return creds_dict    

    def init_db_engine(self, data):
        engine = create_engine(data)
        #reads read_db_cred and initialise and returns an sqalchemy database engine 

    def list_db_tables(self, init_db_engine):
        from sqlalchemy import inspect 
        inspector = inspect(init_db_engine.engine)
        table_names = inspector.get_table_names()
        return table_names

    #useds engine from init_db_engine to list all tables

