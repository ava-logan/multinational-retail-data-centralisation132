import requests
api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
#store_finder = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
store_numbers = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

def list_number_of_stores(url, header):
    response = requests.get(url, headers=header)
    data = response.json()
    print(data)

#list_number_of_stores(store_numbers, api_key)  #response 451
class CreateDataframe:
    def retrieve_stores_data(header):
        import pandas as pd
        column_names = CreateDataframe.get_column_names(header)
        my_dataframe = pd.DataFrame()
        for x in range(2):
            store_finder = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{x}'
            response = requests.get(store_finder, headers=header)
            data = response.json()
            new_data = []
            for key, value in data.items():
                new_data.append(value)
            ready_to_add = pd.DataFrame([new_data], columns=column_names)
            my_dataframe = pd.concat([my_dataframe, ready_to_add], ignore_index=True)
        print(my_dataframe)

    def get_column_names(header):
        source = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/1'
        response = requests.get(source, headers=header)
        data = response.json()
        column_names = []
        for key, value in data.items():
                column_names.append(key)
        return column_names


CreateDataframe.retrieve_stores_data(api_key)

