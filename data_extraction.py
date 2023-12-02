import pandas as pd
import yaml
import database_utils as dbu
from sqlalchemy import text
from database_utils import DatabaseConnector #the methods to connect and extract
from database_utils import yaml_engine as instance_1 #the instance
    

class DataExtractor:
    #Utility class used to extract data from different data sources - included CSv file, API and S3
    # table names are ['legacy_store_details', 'legacy_users', 'orders_table']
    def __init__(self, instance, table_name):
        self.table_name = table_name
        self.instance = instance
    
    def read_rds_table(self):
    #extract the table containing the data and return a pandas dataframe
        conn = instance_1.connect_engine()
        print('Connecting engine...')
        extracted_df = pd.read_sql(self.table_name, conn)
        print(extracted_df)
        print(type(extracted_df))

#connect to engine instance 
#produce a df using table name 

legacy_users_data = DataExtractor(instance_1, 'legacy_users') 
legacy_users_data.read_rds_table()

