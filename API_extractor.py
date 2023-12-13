import requests
api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
#store_finder = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
store_numbers = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

def list_number_of_stores(url, header):
    response = requests.get(url, headers=header)
    data = response.json()
    print(data)

#list_number_of_stores(store_numbers, api_key)  #response 451

def retrieve_stores_data(header):
    import pandas as pd
    my_dataframe = pd.DataFrame()
    for x in range(2):
        store_finder = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{x}'
        response = requests.get(store_finder, headers=header)
        data = response.json()
        new_data = []
        for key, value in data.items():
            new_data.append(value)
        ready_to_add = pd.DataFrame([new_data], columns=None)
        my_dataframe = pd.concat([my_dataframe, ready_to_add], ignore_index=True)
    print(type(my_dataframe))


retrieve_stores_data(api_key)

