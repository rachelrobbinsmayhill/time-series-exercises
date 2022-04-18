# acquire.py

import os
import requests
import pandas as pd


'''
This file contains functions to acquire the items, stores, and sales data from an api.

it also contains a function to acquire the germany power data.
'''



def get_store_data_from_api():
    '''
    
    '''
    response = requests.get('https://api.data.codeup.com/api/v1/stores')
    data = response.json()
    tores = pd.DataFrame(data['payload']['stores'])
    stores = pd.DataFrame(stores)
    return stores



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
    


