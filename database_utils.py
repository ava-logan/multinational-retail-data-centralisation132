import yaml 
import pandas as pd
from sqlalchemy import create_engine

with open('db_creds.yaml', 'r') as file:
    db_creds = yaml.safe_load(file)

with open('sales_data_creds.yaml', 'r') as file:
    sales_data_creds = yaml.safe_load(file)

class DatabaseConnector:
    def __init__(self, data):
        self.data = data
    #used to connect and upload data to database

    def read_db_creds(self):
    #read creds in yaml and return dictionary
    #print(type(db_creds)) returns dict type so i dont know why i need this  
        creds_dict = {}
        for key, value in self.data.items():
            creds_dict[key] = value
        return creds_dict    

    def init_test(self):
        print(f'printing user...{self.user}')

    def init_db_engine(self):
        #required inputs to create an engine 
        #password included here - gitignore?
        #wouldnt work if the keys were not as expected 
        #seems silly to create a method but would be useful for automation
        for key,value in self.data.items():
            if key == "DATABASE_TYPE":
                database_type = value
            if key == "DBAPI":
                dbapi = value 
            if key == "HOST":
                host = value 
            if key == "USER":
                user = value
            if key == "PASSWORD":
                password = value  
            if key == "DATABASE":
                database = value   
            if key == "PORT":
                port = value    

                engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
                print(type(engine))
        
    def connect_engine(self):
        return self.engine.connect()
        #reads read_db_cred and initialise and returns an sqalchemy database engine 

    def list_db_tables(self):
        # table names are ['legacy_store_details', 'legacy_users', 'orders_table']
        #useds engine from init_db_engine to list all tables
        from sqlalchemy import inspect 
        DatabaseConnector.connect_engine(self) #connects to engine
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        print(table_names)
        
            
yaml_engine = DatabaseConnector(db_creds)

sales_data_engine = DatabaseConnector(sales_data_creds)
sales_data_engine.init_db_engine()
