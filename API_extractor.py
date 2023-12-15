import requests
from database_utils import DataCleaning
api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
#store_finder = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
store_numbers = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

def list_number_of_stores(url, header):
    response = requests.get(url, headers=header)
    data = response.json()
    print(data)

#list_number_of_stores(store_numbers, api_key)  #response 451
class APIExtractor:
    def create_dataframe(header):
        import pandas as pd
        column_names = APIExtractor.get_column_names(header)
        my_dataframe = pd.DataFrame()
        for x in range(451):
            store_finder = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{x}'
            response = requests.get(store_finder, headers=header)
            data = response.json()
            new_data = []
            for key, value in data.items():
                new_data.append(value)
            ready_to_add = pd.DataFrame([new_data], columns=column_names)
            my_dataframe = pd.concat([my_dataframe, ready_to_add], ignore_index=True)
        return my_dataframe

    def get_column_names(header):
        source = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/1'
        response = requests.get(source, headers=header)
        data = response.json()
        column_names = []
        for key, value in data.items():
                column_names.append(key)
        return column_names
    
    def clean_api(header):
        data = APIExtractor.create_dataframe(header)
        valid_countries = ['Germany', 'United States', 'United Kingdom']
        data = data[data['country'].isin(valid_countries)]
        country_code_corrections = {'GGB': 'GB', 'DE': 'GM'}
        data.replace(country_code_corrections, inplace=True)
        valid_country_codes = ['GB', 'US', 'GM']
        data = data[data['country_code'].isin(valid_country_codes)]
        return data
        
    def upload_data(header): #change to use output of clean data after test
        from database_utils import DataCleaning
        data = APIExtractor.clean_api(header)
        DataCleaning.upload_to_local(data, 'dim_store_details')
         
#api_table = APIExtractor.create_dataframe(api_key)

api_table.head()

