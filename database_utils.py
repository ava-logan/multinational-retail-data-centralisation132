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

class DataCleaning:
    def clean_legacy_store():
        print('Fetching table...')
        data = pd.read_sql_table('legacy_store_details', yaml_engine.connect_engine())
        data.drop(columns=['lat'], inplace=True)
        print('Dropping silly columns...')
        data.longitude = pd.to_numeric(data.longitude, errors='coerce')
        data.latitude = pd.to_numeric(data.latitude, errors='coerce')
        print('Fixing coordindates....')
        data.opening_date = pd.to_datetime(data.opening_date, infer_datetime_format=True, errors='coerce')
        valid_country_codes = ['DE', 'GB', 'US']
        data = data[data['country_code'].isin(valid_country_codes)]
        print('Checking country codes...')
        valid_store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        data = data[data['store_type'].isin(valid_store_types)]
        continent_corrections = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        data.replace(continent_corrections, inplace=True)
        valid_continents = ['America', 'Europe']
        data = data[data['continent'].isin(valid_continents)]
        print('Done!')
        return data

    def clean_legacy_users():
        print('Fetching table...')
        data = pd.read_sql_table('legacy_users', yaml_engine.connect_engine())
        valid_countries = ['Germany', 'United States', 'United Kingdom']
        data = data[data['country'].isin(valid_countries)]
        print('Checking countries...')
        country_code_corrections = {'GGB': 'GB', 'DE': 'GM'}
        data.replace(country_code_corrections, inplace=True)
        valid_country_codes = ['GB', 'US', 'GM']
        data = data[data['country_code'].isin(valid_country_codes)]
        print('Fixing country codes...')
        data.join_date = pd.to_datetime(data.join_date, infer_datetime_format=True, errors='coerce')
        data.date_of_birth = pd.to_datetime(data.date_of_birth, infer_datetime_format=True, errors='coerce')
        print('Done!')
        return data

    def clean_orders_table():
        data = pd.read_sql_table('orders_table', yaml_engine.connect_engine())
        data.drop(columns=['level_0'], inplace=True)
        data.drop(columns=['1'], inplace=True)
        return data

    def upload_table_to_local(table_name):
        if table_name == 'legacy_store_users':
            data = DataCleaning.clean_legacy_store()
        if table_name == 'legacy_users':
            data = DataCleaning.clean_legacy_users()
        if table_name == 'orders_table':
            data = DataCleaning.clean_orders_table()
        elif table_name != 'legacy_store_users' or 'legacy_users' or 'orders_table':
            print('Check you have typed in the table name correctly')

        data.to_sql(table_name, sales_data_engine.connect_engine(), if_exists='replace')

    def upload_pdf_to_local(data, name):
        data.to_sql(name, sales_data_engine.connect_engine(), if_exists='replace')


