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
        self.engine.connect()

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
        print(extracted_df)

    def upload_to_db(self, dataframe, table_name):
        upload_ready_extracted_df = DatabaseConnector.read_rds_table(self)
        upload_ready_extracted_df.to_sql('self.table_name', self.engine, if_exists='replace')    
        
yaml_engine = DatabaseConnector(db_creds)
sales_data_engine = DatabaseConnector(sales_data_creds)

yaml_engine.read_rds_table('orders_table')
