import yaml 
import pandas as pd
from sqlalchemy import create_engine

with open('db_creds.yaml', 'r') as file: #the creds to access the historal user data
    db_creds = yaml.safe_load(file)

with open('sales_data_creds.yaml', 'r') as file: #creds to connect to sales_data sql database
    sales_data_creds = yaml.safe_load(file)


class DataCleaning:
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
    
    #def clean_user_data():
        #look for NULL values, date errors, incorrectly typed values, wrong information
        #<dataframename>.dropna()
        #.drop_duplicates() - drops exact duplicates 
        #.unique() and .nunique() 


yaml_cleaning = DataCleaning(db_creds)
sales_data_cleaning = DataCleaning(sales_data_creds)