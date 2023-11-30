import yaml 
import sqlalchemy as sa
import pandas as pd
from sqlalchemy import create_engine

with open('db_creds.yaml', 'r') as file:
    db_creds = yaml.safe_load(file)

class DatabaseConnector:
    def __init__(self, data):
        self.data = data
    #used to connect and upload data to database

    def read_db_creds(self, data):
    #read creds in yaml and return dictionary 
        creds_dict = {}
        for key, value in self.data:
            creds_dict[key]=value
        return creds_dict    

    def init_db_engine(self, data, read_db_creds):
        engine_creds_dict = read_db_creds(self.data)
        engine = create_engine(engine_creds_dict)
        print(engine)
        #reads read_db_cred and initialise and returns an sqalchemy database engine 

    def list_db_tables(self, data, init_db_engine):
        from sqlalchemy import inspect 
        inspector = inspect(init_db_engine.engine)
        table_names = inspector.get_table_names()
        return table_names
            #useds engine from init_db_engine to list all tables

print(db_creds)
#yaml_engine = DatabaseConnector(creds_data)
#yaml_engine.read_db_creds()