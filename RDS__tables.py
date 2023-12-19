import yaml 
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect 
from SQL_connector import SQLConnector

with open('db_creds.yaml', 'r') as file: #the creds to access the historal user data
    db_creds = yaml.safe_load(file)

class CleanRDS:
    def clean_legacy_store_details(data):
        data.drop(columns=['lat'], inplace=True)
        #data.longitude = pd.to_numeric(data.longitude, errors='coerce')
        #data.latitude = pd.to_numeric(data.latitude, errors='coerce')
        valid_country_codes = ['DE', 'GB', 'US']
        data = data[data['country_code'].isin(valid_country_codes)]
        valid_store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        data = data[data['store_type'].isin(valid_store_types)]
        continent_corrections = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        data.replace(continent_corrections, inplace=True)
        valid_continents = ['America', 'Europe']
        data = data[data['continent'].isin(valid_continents)]
        return data

    def clean_legacy_users(data):
        country_code_corrections = {'GGB': 'GB'}
        data.replace(country_code_corrections, inplace=True)
        change_germany_code = {'DE': 'GM'}
        data.replace(change_germany_code, inplace=True)        
        valid_countries = ['Germany', 'United States', 'United Kingdom']
        data = data[data['country'].isin(valid_countries)]
        valid_country_codes = ['GB', 'US', 'GM']
        data = data[data['country_code'].isin(valid_country_codes)]
        return data

    def clean_orders_table(data):
        data.drop(columns=['level_0'], inplace=True)
        data.drop(columns=['1'], inplace=True)
        data.drop(columns=['first_name'], inplace=True)
        data.drop(columns=['last_name'], inplace=True)
        return data    

class RDSTable:
    def __init__(self, data, table_name):
        self.data = data 
        self.table_name = table_name
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
        
    def list_db_tables(self): # table names are ['legacy_store_details', 'legacy_users', 'orders_table']
        self.engine.connect()
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names 
    
    def read_rds_table(self):   
        data = pd.read_sql_table(self.table_name, self.engine.connect())
        return data

    def upload_table_to_local(self):
        if self.table_name == 'legacy_store_details':
            data = RDSTable.read_rds_table(self)
            data = CleanRDS.clean_legacy_store_details(data)
        if self.table_name == 'legacy_users':
            data = RDSTable.read_rds_table(self)
            data = CleanRDS.clean_legacy_users(data)
            SQLConnector.upload_to_local(data, 'dim_user_2')
        if self.table_name == 'orders_table':
            data = RDSTable.read_rds_table(self)
            data = CleanRDS.clean_orders_table(data)
        
        #SQLConnector.upload_to_local(data, self.table_name)  


orders_table_instance = RDSTable(db_creds, 'orders_table')
legacy_store_details_instance = RDSTable(db_creds, 'legacy_store_details')
legacy_users_instance = RDSTable(db_creds, 'legacy_users')


#orders_table_instance.upload_table_to_local()
#legacy_store_details_instance.upload_table_to_local()
legacy_users_instance.upload_table_to_local()
   