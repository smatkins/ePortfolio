# Local Climatological Data from NOAA NCEI API
## The Function Produces A Pandas Dataframe With Weather Values 
### The Code Below The Function Produces Nighttime Low and Daytime High Temp 
### This code take a while to run, so if low and high temp are not needed, comment them out 

 
from datetime import datetime, time
import matplotlib.pyplot as plt
from time import strptime
import requests
import json
import pandas as pd
import itertools

#######################################################################
# API To Produce Weather Dataframe
#######################################################################

def weather_data_for_stations(dataset = 'local-climatological-data',
                        stationList = [''], 
                        startDateList = [''],
                        endDateList = [''],
                        dataTypesList = [''], 
                        format = 'json',
                        units = '',
                        includeAttributes = '', 
                        includeStationName = '', 
                        includeStationLocation = ''):

##############################################################################################################

#    NCEI Data Service API User Documentation: https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation

#    PARAMETERS:
#    -----------
#    #NOTE: API Can Only Process 50 Station Years at a Time

#    dateset (required): the dataset from NOAA NCEI you want to analyze. Not all datasets available at NCEI are available in the API.
#               For local climatological data (LCD) (usually 1-hour granualarity), type 'local-climatological-data'

#    stationList (required): To find station IDs go to https://www.ncdc.noaa.gov/cdo-web/datatools/lcd and
#              use the tool provided to find stations. Click the station you want the ID for. 
#              Scroll down to View Station Data, input any year, month, and day, click View Data.
#              In the top left corner in bold, there will be 'WBAN:' and then an eleven digit 
#              number; the eleven digit number is the station ID
#              NOTE: A list of station IDs for all stations collecting LCD data, as well as the time period
#                    those stations collected data, in the Hampton Roads can be found here:
#                    https://github.com/JeffersonLab/jlab_datascience_jiacm4hcs/blob/main/data/Hampton%20Roads/climate/metadata/local_climatological/noaa_lcd_hr_station_list.txt

#    startDateList (required): must be in the format YYYY-MM-DD or YYYY-MM-DDTHH-MM-SSZ (the T and Z must be included)
#    endDateList (required): must be in the same format as 'startDate' and must come after 'startDate'
#              NOTE: API can only process 50 station years at a time, requesting more station hours will result in a 400 error message 
#              Example: startDateList = ['1970-01-01', 1973-01-01]; endDateList = ['1972-12-31', '2022-31-12']

#    dataTypesList: variable name you want to analyze
#              NOTE: Case matters 
#              NOTE: If left empty, the code will return all variables
#              NOTE: A list of all hourly LCD variables can be found here: 
#                     https://github.com/JeffersonLab/jlab_datascience_jiacm4hcs/blob/main/data/Hampton%20Roads/climate/metadata/local_climatological/noaa_lcd_data_type_list.txt

#    format: data format you want delivered (csv, json, netcdf, pdf, ssv)
#              NOTE: this code needs the format to be 'json'

#    units: units you want the weather data delivered in (metric or standard)

#    includeAttribute: 0 (false) to not include data type attributes, 1 (true) to include data type attributes

#    includeStationName: 0 (false) to not include station name, 1 (true) to include station name

#    includeStationLocation: 0 (false) to not include station location, 1 (true) to include staion location

#    -------------
#    RETURN:
#    -------------
#    Aggregated dataframe of stations, time, and variables 

##############################################################################################################

# create request url
    dataframe = []
    for station in stationList:
        for dataTypes in dataTypesList:
            for startDate,endDate in zip(startDateList,endDateList):
                baseURL = 'https://www.ncei.noaa.gov/access/services/data/v1'
                url = baseURL + '?dataset=' + dataset
                if stationList != '':
                    url += '&stations=' + station
                if startDateList != '':
                    url += '&startDate=' + startDate
                if endDateList != '':
                    url += '&endDate=' + endDate
                if dataTypesList != '':
                    url += '&dataTypes=' + dataTypes
                if format != '':
                    url += '&format=' + format
                if units != '':
                    url += '&units=' + units
                if includeAttributes != '':
                    url += '&includeAttribute=' + includeAttributes
                if includeStationName != '':
                    url += '&includeStationName=' + includeStationName
                if includeStationLocation != '':
                    url += '&includeStationLocation=' + includeStationLocation
    
                response = requests.get(url)
                json_stations = json.loads(response.text)
                df = pd.DataFrame(json_stations)

                dataframe.append(df)
    return pd.concat(dataframe).astype(str).groupby(['STATION','DATE'], as_index = False).min()

weather = weather_data_for_stations()

###############################################################
#Daytime High and Nighttime Low
###############################################################
# NOTE: Sunrise, sunset, and hourlydrybulbtemperature need to be called in the function to get daytime high and nighttime low temps
#       If any of the above variables are not called, column will contain all 'nan' values

#get the unique dates
dates = []
for i in range(0, len(weather)):
    date = weather.iloc[i]['DATE'][0:11]
    dates.append(date)
#use 'print(set(dates))' to see unique days
stations = []
#get the unique stations
for row in range(0, len(weather)):
    station = weather.iloc[row]['STATION']
    stations.append(station)
#use 'print(set(stations))' to see unique stations

#creates subsets for each unique date (day) and station
subsets = []
for station in set(stations):
  for date in set(dates):
    subset = weather[weather['DATE'].str.startswith(date) & weather['STATION'].str.startswith(station)]
    subsets.append(subset)

# calculates daytime high and nighttime low within each subset
daytemps = []
nighttemps = []
for subset in subsets:
    if 'Sunrise' in subset.columns.values and 'Sunset' in subset.columns.values and 'HourlyDryBulbTemperature' in subset.columns.values:
        if subset.iloc[-1]['Sunset'] != 'nan' and subset.iloc[-1]['Sunrise'] != 'nan':
            sunset = subset.iloc[-1]['DATE'][0:11] + datetime.strftime(datetime.strptime(str(pd.to_datetime(subset.iloc[-1]['Sunset'], format='%H%M')), '%Y-%m-%d %H:%M:%S'),format = '%H:%M:%S')
            sunrise = subset.iloc[-1]['DATE'][0:11] + datetime.strftime(datetime.strptime(str(pd.to_datetime(subset.iloc[-1]['Sunrise'], format='%H%M')), '%Y-%m-%d %H:%M:%S'),format = '%H:%M:%S')
            for row in range(0,len(subset)):
                if subset.iloc[row]['DATE'] > sunrise and subset.iloc[row]['DATE'] < sunset:
                    daytemp = subset.iloc[row]['HourlyDryBulbTemperature']
                    daytemps.append(daytemp)
                    subset['Daytime_High'] = list(itertools.repeat('nan', len(subset)-1)) + [max(daytemps)]
                if subset.iloc[row]['DATE'] < sunrise or subset.iloc[row]['DATE'] > sunset:
                    nighttemp = subset.iloc[row]['HourlyDryBulbTemperature']
                    nighttemps.append(nighttemp)
                    subset['Nighttime_Low'] = list(itertools.repeat('nan', len(subset)-1)) + [min(nighttemps)]
        else:
            subset['Daytime_High'] = list(itertools.repeat('nan', len(subset)))
            subset['Nighttime_Low'] = list(itertools.repeat('nan', len(subset)))
    else:
        subset['Daytime_High'] = list(itertools.repeat('nan', len(subset)))
        subset['Nighttime_Low'] = list(itertools.repeat('nan', len(subset)))

weather = pd.concat(subsets)

#####################################################################################
# Apparent Temperature
#####################################################################################
# NOTE: to calculate apparent temperature, dry bulb temp and relative humidity need to be called in function
#       If any of the above variables are not called, column will contain all 'nan' values

#calculating wind chill and appending it to dataframe
windchills = []
for row in range(0, len(weather)):
    if 'HourlyDryBulbTemperature' in weather.columns.values and 'HourlyWindSpeed' in weather.columns.values:
        if weather.iloc[row]['HourlyDryBulbTemperature'] != 'nan' and weather.iloc[row]['HourlyWindSpeed'] != 'nan':
            windchill = 35.74 + 0.6215*int(weather.iloc[row]['HourlyDryBulbTemperature']) - 0.3574*(int(weather.iloc[row]['HourlyWindSpeed']) ** 0.16) + 0.4275 * ((int(weather.iloc[row]['HourlyDryBulbTemperature'])*int(weather.iloc[row]['HourlyWindSpeed'])) ** 0.16)
        else:
            windchill = 'nan'
        windchills.append(windchill)
    else:
        windchills = list(itertools.repeat('nan',len(weather)))
weather['HourlyWindChill'] = windchills

# calculating apparent temp and appending it to dataframe
apparents = []
for row in range(0, len(weather)):
    if 'HourlyDryBulbTemperature' in weather.columns.values and 'HourlyRelativeHumidity'in weather.columns.values:
        if weather.iloc[row]['HourlyDryBulbTemperature'] != 'nan' and weather.iloc[row]['HourlyRelativeHumidity'] != 'nan':
            apparent = -42.379 + 2.04901523*(int(weather.iloc[row]['HourlyDryBulbTemperature'])) + 10.14333127*(int(weather.iloc[row]['HourlyRelativeHumidity'])) - 0.22475541*(int(weather.iloc[row]['HourlyRelativeHumidity'])*int(weather.iloc[row]['HourlyDryBulbTemperature'])) - 6.83783*(10**-3)*(int(weather.iloc[row]['HourlyDryBulbTemperature'])**2) - 5.481717*(10**-2)**(int(weather.iloc[row]['HourlyRelativeHumidity'])**2) + 1.22874*(10**(-3))*(int(weather.iloc[row]['HourlyDryBulbTemperature'])**2)*(int(weather.iloc[row]['HourlyRelativeHumidity'])) + 8.5282*(10**(-4))*(int(weather.iloc[row]['HourlyDryBulbTemperature']))*(int(weather.iloc[row]['HourlyRelativeHumidity'])**(-2)) - 1.99*(10**(-6))*(int(weather.iloc[row]['HourlyDryBulbTemperature'])**2)*(int(weather.iloc[row]['HourlyRelativeHumidity'])**2)
        else:
            apparent = 'nan'
        apparents.append(apparent)
    else:
        apparents = list(itertools.repeat('nan', len(weather)))
weather['HourlyApparentTemperature'] = apparents

####################################################################
# Final Dataframe With All Calculations
####################################################################

print(weather)

