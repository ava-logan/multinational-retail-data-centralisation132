import yaml 
import pandas as pd
from sqlalchemy import create_engine

with open('db_creds.yaml', 'r') as file: #the creds to access the historal user data
    db_creds = yaml.safe_load(file)

with open('sales_data_creds.yaml', 'r') as file: #creds to connect to sales_data sql database
    sales_data_creds = yaml.safe_load(file)

class DatabaseConnector:
    def __init__(self, data):
        self.data = data 
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


    def read_db_creds(self):
    #read creds in yaml and return dictionary
    #print(type(db_creds)) returns dict type - needed to format other sources?
        creds_dict = {}
        for key, value in self.data.items():
            creds_dict[key] = value
        return creds_dict

    def list_db_tables(self):
        # table names are ['legacy_store_details', 'legacy_users', 'orders_table']
        #useds engine from init_db_engine to list all tables
        from sqlalchemy import inspect 
        print(type(self.engine)) #connects to engine
        #inspector = inspect(self.engine)
        #table_names = inspector.get_table_names()
        #print(table_names)
        
            
yaml_engine = DatabaseConnector(db_creds)
sales_data_engine = DatabaseConnector(sales_data_creds)

yaml_engine.list_db_tables()
