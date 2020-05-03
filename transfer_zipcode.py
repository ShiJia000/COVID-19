import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import googlemaps
from collections import defaultdict

df = pd.read_csv('/Users/xilinjiang/Desktop/turnstile_200411.csv')

maps_api = 'AIzaSyAoc0zhpPqKxu3Se78P5BcArtURbPcrac0'

gmaps = googlemaps.Client(key=maps_api)
stations = df['STATION'].unique()

ny_stations = list(map(lambda x: x + ', New York, NY', stations))
unique_geocodes = defaultdict(str)

download=True

if download==True:
    for address in ny_stations:
        geocode = gmaps.geocode(address)
        print(geocode)
        unique_geocodes[address] = geocode

print(unique_geocodes)

parse=True

if parse==True:
    zipcodes = []
    stations = []
    
    for geocode in unique_geocodes:
        try:
            address = unique_geocodes[geocode][0]['address_components']
            if address[len(address) - 1]['types'] == ['postal_code']:
                zipcodes.append(address[len(address)- 1]['long_name'])
                stations.append(geocode.split(',')[0])
        except:
            print('Unable to find location data')
        
station_zips = pd.DataFrame(columns=['STATION', 'zipcode'])
station_zips['STATION'] = stations
station_zips['zipcode'] = zipcodes


station_zips.to_csv('station_zipcode.csv')


