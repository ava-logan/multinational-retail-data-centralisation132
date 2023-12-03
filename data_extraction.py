import pandas as pd
import yaml
from database_utils import DatabaseConnector


class DataExtractor:
    #Utility class used to extract data from different data sources - included CSv file, API and S3
    # table names are ['legacy_store_details', 'legacy_users', 'orders_table']
    def __init__(self, instance, table_name):
        self.table_name = table_name
        self.instance = instance

    def upload_to_db(self):
        engine = self.instance.connect_engine()
        upload_ready_extracted_df = DataExtractor.read_rds_table(self)
        upload_ready_extracted_df.to_sql(f'{self.table_name}', engine, if_exists='replace')

