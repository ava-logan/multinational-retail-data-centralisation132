import requests
api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_finder = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/5'
store_numbers = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

def list_number_of_stores(url, header):
    response = requests.get(url, headers=header)
    data = response.json()
    print(data)

#list_number_of_stores(store_numbers, api_key)  #response 451

def retrieve_stores_data(url, header):
    response = requests.get(url, headers=header)
    data = response.json()
    print(data)

retrieve_stores_data(store_finder, api_key)
    