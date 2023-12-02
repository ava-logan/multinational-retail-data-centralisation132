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
        engine = self.instance.connect_engine()
        print('Connecting engine...')
        extracted_df = pd.read_sql(self.table_name, engine)
        return extracted_df

    def upload_to_db(self):
        engine = self.instance.connect_engine()
        upload_ready_extracted_df = DataExtractor.read_rds_table(self)
        upload_ready_extracted_df.to_sql(f'{self.table_name}', engine)

#connect to engine instance 
#produce a df using table name 

legacy_users_data = DataExtractor(instance_1, 'legacy_users') 
legacy_users_data.upload_to_db()

