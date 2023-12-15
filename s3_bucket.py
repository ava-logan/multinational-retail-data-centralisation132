#product info is in cvs format in an s3 bucket in aws
# create extract_from_s3 method: uses boto3 package to download and extract the information returning a pandas DataFrame.
#S3 address: s3://data-handling-public/products.csv the method will take this address in as an argument and return the pandas DataFrame.
#You will need to be logged into the AWS CLI before you retrieve the data from the bucket.
#Create convert_product_weights method: will take the products DataFrame as an argument and return the products DataFrame.
#If you check the weight column in the DataFrame the weights all have different units.
#Convert all weight to decimal in kg. Use a 1:1 ratio of ml to g as a rough estimate for the rows containing ml.
#Develop the method to clean up the weight column and remove all excess characters then represent the weights as a float.
#create method: clean_products_data this method will clean the DataFrame of any additional erroneous values.
#upload data to sales_data as a table named dim_products.
import boto3
import pandas as pd
from database_utils import DataCleaning

class GetBuckets:
    def extract_from_s3():
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'products.csv', '/Users/avalogan/Desktop/aicore/retail_github/bucket.csv')
    
    def convert_to_df():
        dataframe = pd.read_csv('/Users/avalogan/Desktop/aicore/retail_github/s3bucket.csv')
        return dataframe
    
    def clean_the_frame(dataframe):
        valid_categories = ['diy', 'food-and-drink', 'health-and-beauty', 'homeware', 'pets', 'sports-and-leisure', 'toys-and-games']
        dataframe = dataframe[dataframe['category'].isin(valid_categories)]
        dataframe['weight'] = dataframe['weight'].apply(GetBuckets.WeightConverter.select_path)
        return dataframe

    def upload_bucket():
        dataframe = GetBuckets.convert_to_df()
        dataframe = GetBuckets.clean_the_frame(dataframe)
        DataCleaning.upload_to_local(dataframe, 'dim_products')

    class WeightConverter:
        def fix_multiplications(weight): 
            x = weight
            end = x.index('g')
            num_str = x[:end]
            num_format = num_str.replace('x', '*')
            num_int = eval(num_format)
            kilo_format = num_int / 1000
            return kilo_format

        def convert_grams(weight):
            x = weight
            weight_str = x[0:-1]
            weight_int = float(weight_str)
            weight_in_kilos = weight_int / 1000
            return weight_in_kilos

        def convert_mls(weight):
            x = weight
            weight_str = x[0:-2]
            weight_int = float(weight_str)
            weight_in_kilos = weight_int / 1000
            return weight_in_kilos  

        def convert_kilos(weight):
            x = weight 
            weight = x[0:-2]
            weight_int = float(weight)
            return weight_int

        def select_path(weight):
            if 'x' in weight:
                format_weight = GetBuckets.WeightConverter.fix_multiplications(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            elif weight == '77g .':
                format_weight = 0.077
                return format_weight
            elif 'ml' in weight:
                format_weight = GetBuckets.WeightConverter.convert_mls(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            elif 'g' in weight and 'k' not in weight:
                format_weight = GetBuckets.WeightConverter.convert_grams(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            elif 'kg' in weight:
                format_weight = GetBuckets.WeightConverter.convert_kilos(weight)
                rounded_weight = round(format_weight, 3)
                return rounded_weight
            else: 
                return "NOT AVALIABLE"


#test_weight = GetBuckets.WeightConverter.select_path('5dsfg')
#print(test_weight)


dataframe = GetBuckets.upload_bucket()
