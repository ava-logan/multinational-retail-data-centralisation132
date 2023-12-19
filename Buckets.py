import boto3
import pandas as pd
from SQL_connector import SQLConnector
s3 = boto3.client('s3')

date_bucket_creds = {'name': 'data-handling-public', 'object_key': 'date_details.json', 'local_path': '/Users/avalogan/Desktop/aicore/retail_github/date_details.json', 'sql_name': 'dim_date_times'}
products_bucket_creds = {'name': 'data-handling-public', 'object_key': 'products.csv', 'local_path': '/Users/avalogan/Desktop/aicore/retail_github/s3bucket.csv', 'sql_name': 'dim_products'}

class BucketCleaner:

    def clean_products_bucket(df):  
        valid_categories = ['diy', 'food-and-drink', 'health-and-beauty', 'homeware', 'pets', 'sports-and-leisure', 'toys-and-games']
        df['catergory'] = df['category'].isin(valid_categories)
        df['formatted_weight'] = None
        df['formatted_weight'] = df['weight'].apply(lambda x: BucketCleaner.select_path(x))
        return df
        
    def clean_date_bucket(df):
        valid_time_periods = ['Late_Hours', 'Morning', 'Midday', 'Evening']
        df = df[df['time_period'].isin(valid_time_periods)]
        valid_months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
        df = df[df['month'].isin(valid_months)]
        return df 
    
    def fix_multiplications(weight): 
        end = weight.index('g')
        num_str = weight[:end]
        num_format = num_str.replace('x', '*')
        num_int = eval(num_format)
        num_str = str(num_int)
        num_str_method_format = f'{num_str}g'
        num_kilos = BucketCleaner.convert_grams(num_str_method_format)
        return num_kilos

    def convert_grams(weight):
        weight_str = weight[0:-1]
        weight_int = float(weight_str)
        weight_in_kilos = weight_int / 1000
        return weight_in_kilos
    
    def select_path(weight):
        if type(weight) == float:
            return 'NOT AVALIABLE'
        elif 'x' in weight:
            format_weight = BucketCleaner.fix_multiplications(weight)
            return format_weight
        elif weight == '77g .':
            return 0.077
        elif 'g' in weight and 'kg' not in weight:
            format_weight = BucketCleaner.convert_grams(weight)
            return format_weight
        elif 'ml' in weight:
            new_weight = weight.replace('ml', 'g')
            format_weight = BucketCleaner.convert_grams(new_weight)
            return format_weight
        elif 'kg' in weight:
            format_weight = weight.replace('kg', '')
            return format_weight
        else: 
            return f'weight {weight} not changable'


class Buckets:
    def __init__(self, **args):
        for key, value in args.items():
            if key == 'name':
                self.name = value
            if key == 'object_key':
                self.object_key = value
            if key == 'local_path':
                self.local_path = value
            if key == 'sql_name':
                self.sql_name = value
        
    def extract_bucket(self): #only needs to be downloaded once
        s3.download_file(self.name, self.object_key, self.local_path)
    
    def convert_to_df(self):
        df = pd.read_csv(self.local_path)
        return df

    def upload_bucket(self):
        if self.sql_name == 'dim_date_times':
            data = pd.read_json(self.local_path)
            clean_data = BucketCleaner.clean_date_bucket(data)
            SQLConnector.upload_to_local(clean_data, 'date_checker')
        if self.sql_name == 'dim_products':
            data = pd.read_csv(self.local_path)
            clean_data = BucketCleaner.clean_products_bucket(data)
            SQLConnector.upload_to_local(clean_data, 'products_3')


date_bucket_instance = Buckets(**date_bucket_creds)
#products_bucket_instance = Buckets(**products_bucket_creds)
date_bucket_instance.upload_bucket()
