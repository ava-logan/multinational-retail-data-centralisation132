#The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes.

#The file is currently stored on S3 and can be found at the following link https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json.

#Extract the file and perform any necessary cleaning, then upload the data to the database naming the table dim_date_times.
import boto3
import pandas as pd
from database_utils import DataCleaning

class SalesBucket:
    def sales_extract_from_s3():
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'date_details.json', '/Users/avalogan/Desktop/aicore/retail_github/date_details.json')
    
    def sales_convert_and_clean():
        data = pd.read_json('/Users/avalogan/Desktop/aicore/retail_github/date_details.json')
        valid_time_periods = ['Late_hours', 'Morning', 'Midday', 'Evening']
        data = data[data['time_period'].isin(valid_time_periods)]
        valid_months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
        data = data[data['month'].isin(valid_months)]
        DataCleaning.upload_to_local(data, 'dim_date_times')


SalesBucket.sales_convert_and_clean()