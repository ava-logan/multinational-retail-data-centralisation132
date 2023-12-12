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

    def connect_engine(self):
        return self.engine.connect()

    def read_db_creds(self): #print(type(db_creds)) returns dict type - needed to format other sources?
        creds_dict = {}
        for key, value in self.data.items():
            creds_dict[key] = value
        return creds_dict

    def list_db_tables(self): # table names are ['legacy_store_details', 'legacy_users', 'orders_table']
        from sqlalchemy import inspect 
        self.engine.connect() #connects to engine
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names 

    def read_rds_table(self, table_name):   
        extracted_df = pd.read_sql_table(table_name, self.engine)
        return extracted_df
    

yaml_engine = DatabaseConnector(db_creds) #the engine to connect to aws 
sales_data_engine = DatabaseConnector(sales_data_creds) #the engine to connect to localhost sql

def upload_table_to_local(ul_table_name):
    ul_extracted_df = yaml_engine.read_rds_table(ul_table_name) #1. use aws engine 2. use table name in argument read  table 
    print('Table read...')
    ul_extracted_df.dropna()     
    print('Nulls dropped...')                               #3. drops null values
    ul_extracted_df = ul_extracted_df.drop_duplicates()          #4. drop exact duplicates
    print('Duplicates dropped...')
    ul_extracted_df.to_sql(f'{ul_table_name}', sales_data_engine.connect_engine(), if_exists='replace') #5. upload table to sql using 6. localhost engine

upload_table_to_local('legacy_store_details')