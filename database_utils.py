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
        self.database_type = 0
        self.dbapi = 0 
        self.host = 0 
        self.user = 0
        self.password = 0  
        self.database = 0   
        self.port = 0
        self.engine = 0
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
                self.database_type = value
            if key == "DBAPI":
                self.dbapi = value 
            if key == "HOST":
               self.host = value 
            if key == "USER":
                self.user = value
            if key == "PASSWORD":
                self.password = value  
            if key == "DATABASE":
                self.database = value   
            if key == "PORT":
                self.port = value    

                self.engine = create_engine(f"{self.database_type}+{self.dbapi}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")
                print(self.engine)
        
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
