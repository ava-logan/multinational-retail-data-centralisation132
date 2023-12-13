import requests
api_key = {'api_key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_finder = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
store_numbers = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

def list_number_of_stores(url, header):
    response = requests.get(url, header)
    print(response.status_code)

#list_number_of_stores(store_numbers, api_key)  #response 403

def retrieve_stores_data(url, header):
    response = requests.get(url, header)
    print(response.status_code)


response = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores?api_key=yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX')
print(response.status_code)