'''
This module contains all aspects of data acquisition for the sales, stores, and items endpoints within https://api.data.codeup.com/api/v1

- get_store_item_demand_data() - will acquire all sales, store, and item data and merge it into one dataframe.

If you want to get the individual datasets from the store item demand data you
can do so with
- get_items_data
- get_stores_dat
- get_sales_data

- get_opsd_data() - will acquire all of the open power systems data for Germany and create a dataframe.
'''
import os
import requests
import pandas as pd


def get_store_data_from_api():

    response = requests.get('https://api.data.codeup.com/api/v1/stores')
    data = response.json()
    stores = pd.DataFrame(data['payload']['stores'])
    stores = pd.DataFrame(stores)
    return stores


def get_items_data_from_api():    

    domain = 'https://api.data.codeup.com'
    # path or where we are accessing the data inside the url
    endpoint = '/api/v1/items'
    #create an empty list to place the data within
    items = []

    while endpoint is not None:
        #assign url
        url = domain + endpoint
        # print(f'Acquiring page {endpoint}')
        # making a request and storing the response to the request as a string
        response = requests.get(url)
        # taking the json data and converting to Pandas list of dictionaries (python)
        data = response.json()
        # .extend adds elements from a list to another list
        items.extend(data['payload']['items'])
        # reasigning the endpoint variable to have the path to the next page.
        endpoint = data['payload']['next_page']

    items = pd.DataFrame(items)
    return items


def get_sales_data_from_api():
    
    domain = 'https://api.data.codeup.com'
    # path or where we are accessing the data inside the url
    endpoint = '/api/v1/items'
    #create an empty list to place the data within
    sales = []
    
    while endpoint is not None:
        #assign url
        url = domain + endpoint
        # print(f'Acquiring page {endpoint}')
        # making a request and storing the response to the request as a string
        response = requests.get(url)
        # taking the json data and converting to Pandas list of dictionaries (python)
        data = response.json()
        # .extend adds elements from a list to another list
        items.extend(data['payload']['items'])
        # reasigning the endpoint variable to have the path to the next page.
        endpoint = data['payload']['next_page']

    items = pd.DataFrame(items)
    return items



def get_store_data():
    
    filename = 'stores.csv'

    if os.path.exists(filename):
        print('Reading from csv file...')
        return pd.read_csv(filename)
    else:
        df = get_stores_data_from_api()
        df.to_csv(filename)
    return df   


def get_items_data():
    
    filename = 'items.csv'

    if os.path.exists(filename):
        print('Reading from csv file...')
        return pd.read_csv(filename)
    else:
        df = get_items_data_from_api()
        df.to_csv(filename)
    return df   


def get_sales_data():
    
    filename = 'sales.csv'

    if os.path.exists(filename):
        print('Reading from csv file...')
        return pd.read_csv(filename)
    else:
        df = get_sales_data_from_api()
        df.to_csv(filename)
    return df  


def get_store_item_demand_data():
    
    sales = get_sales_data()
    items = get_items_data()
    stores = get_stores_data()
    
    sales = sales.rename(columns={'item': 'item_id', 'store': 'store_id'})
    df = pd.merge(sales, items, how='left', on='item_id')
    df = pd.merge(df, items, how='left', on='item_id')

    return df





def get_opsd_data():
    if os.path.exists('opsd.csv'):
        return pd.read_csv('opsd.csv')
    df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    df.to_csv('opsd.csv', index=False)
    return df











def new_germany_power_data():
    '''
    THIS FUNCTION ACQUIRES THE OPEN POWER SYSTEMS DATA (OPSD) FOR GERMANY WHICH INCLUDES COUNTRY-WIDE TOTALS OF 
    ELECTRICITY CONSUMPTION, WIND POWER PRODUCTION, AND SOLAR POWER PRODUCTION FOR 2006-2017. IT READS A .CSV 
    INTO A DATAFRAME, THEN WRITES IT TO A .CSV FILE.
    '''
    opsd = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    print('Saving to .csv file.')
    opsd.to_csv('opsd_germany_power_data.csv', index=False)
    return opsd



def get_germany_power_data():
    '''
    THIS FUNCTION VERIFIES WHETHER THE OPSD FOR GERMANY IS SAVED LOCALLY IN A .CSV FILE. IF NOT, IT ACQUIRES
    THE DATA USING THE NEW_GERMANY_POWER_DATA FUNCTION, OTHERWISE IT ACCESSES THE DATA FROM THE LOCAL .CSV.
    '''
    
    if os.path.isfile('opsd_germany_power_data.csv') == False:
        print('Acquiring fresh data...')
        opsd = new_germany_power_data()
    else:
        print('Reading from .csv file.')
        opsd = pd.read_csv('opsd_germany_power_data.csv')
    print('Data acquisition complete.')
    return opsd

    


