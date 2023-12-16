import boto3
import pandas as pd
from database_utils import DataCleaning
s3 = boto3.client('s3')

date_bucket_creds = {'name': 'data-handling-public', 'object_key': 'date_details.json', 'local_path': '/Users/avalogan/Desktop/aicore/retail_github/date_details.json', 'sql_name': 'dim_date_times'}
products_bucket_creds = {'name': 'data-handling-public', 'object_key': 'products.csv', 'local_path': '/Users/avalogan/Desktop/aicore/retail_github/bucket.csv', 'sql_name': 'dim_products'}

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
    
    def clean_products_bucket(self):
        df = Buckets.convert_to_df(self)
        valid_categories = ['diy', 'food-and-drink', 'health-and-beauty', 'homeware', 'pets', 'sports-and-leisure', 'toys-and-games']
        df = df[df['category'].isin(valid_categories)]
        df['weight'] = df['weight'].apply(Buckets.WeightConverter.select_path)
        return df
    
    def clean_date_bucket(self):
        df = Buckets.convert_to_df(self)
        valid_time_periods = ['Late_hours', 'Morning', 'Midday', 'Evening']
        df = df[df['time_period'].isin(valid_time_periods)]
        valid_months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
        df = df[df['month'].isin(valid_months)]
        return df    

    def upload_bucket(self):
        if self.sql_name == 'dim_date_times':
            df = Buckets.clean_date_bucket(self)
        if self.sql_name == 'dim_products':
            df = Buckets.clean_products_bucket(self)
        DataCleaning.upload_to_local(df, self.sql_name)

    class WeightConverter:
        def fix_multiplications(weight): 
            end = weight.index('g')
            num_str = weight[:end]
            num_format = num_str.replace('x', '*')
            num_int = eval(num_format)
            kilo_format = num_int / 1000
            return kilo_format

        def convert_grams(weight):
            weight_str = weight[0:-1]
            weight_int = float(weight_str)
            weight_in_kilos = weight_int / 1000
            return weight_in_kilos

        def convert_mls(weight):
            weight_str = weight[0:-2]
            weight_int = float(weight_str)
            weight_in_kilos = weight_int / 1000
            return weight_in_kilos  

        def convert_kilos(weight):
            weight_str = weight[0:-2]
            weight_int = float(weight_str)
            return weight_int

        def select_path(weight):
            if 'x' in weight:
                format_weight = Buckets.WeightConverter.fix_multiplications(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            elif weight == '77g .':
                format_weight = 0.077
                return format_weight
            elif 'ml' in weight:
                format_weight = Buckets.WeightConverter.convert_mls(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            elif 'g' in weight and 'k' not in weight:
                format_weight = Buckets.WeightConverter.convert_grams(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            elif 'kg' in weight:
                format_weight = Buckets.WeightConverter.convert_kilos(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            else: 
                return None

date_bucket_instance = Buckets(**date_bucket_creds)
products_bucket_creds = Buckets(**products_bucket_creds)