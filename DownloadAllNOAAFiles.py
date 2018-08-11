years = ['2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018']

# # Imports


import pandas as pd
import numpy as np
import os
from os.path import isfile, join
from os import listdir
import math
from pandas import ExcelWriter
import ftplib
import struct
import datetime
import math
import gzip
import requests


# # Reading Population Weighted Data


population_dataframe = pd.read_excel('PopulationByFIPS.xlsx')
population_dataframe.index = [str(fips).zfill(5) for fips in population_dataframe['Fips']]
population_dataframe.drop('Fips', axis=1, inplace=True)
population_dataframe.drop('County', axis=1, inplace=True)
population_dataframe.drop('State', axis=1, inplace=True)
#population_dataframe['Fips Weight'] = population_dataframe['Population'] / np.sum(population_dataframe['Population'])
fips_pop_dict = list(population_dataframe.to_dict().values())[0]


# # Functions


def getCountry(station_id):

    with open('AllStations.txt', 'r') as stations:

        for line in stations.readlines():

            if line[:6] == station_id:

                stations.close()
                return line[43:45], line[48:50]

            else:

                pass

    stations.close()

def station_lat_long(stid):

    '''
    Using station id to find station latitude and longitude
    Returns tuple of lat, long
    '''

    found = False

    with open('AllStations.txt', 'r') as stations:

        for line in stations.readlines():

            if not found:

                if stid == line[:6]:

                    return float(line[56:64]), float(line[65:73])

                    found = True

                else:

                    pass

            else:

                break

    stations.close()

def get_fips(latitude, longitude):

    '''
    Uses https://geo.fcc.gov/api/census/ api to get FIPS code for that location
    '''

    try:

        response = requests.get('https://geo.fcc.gov/api/census/block/find?latitude='+str(latitude)+'&longitude='+str(longitude)+'&format=json')

        fips = str(response.json()['County']['FIPS']).zfill(5)

        return fips

    except:

        return None


# # DIRS


BASE_DIR = str(os.getcwd())

EXCELS_DIR = os.path.join(BASE_DIR + os.sep + 'DataExcels')

if not os.path.exists(EXCELS_DIR):

        os.makedirs(EXCELS_DIR)

YEARLY_WEIGHTED_DIR = os.path.join(BASE_DIR + os.sep + 'YearlyWeighted')

if not os.path.exists(YEARLY_WEIGHTED_DIR):

        os.makedirs(YEARLY_WEIGHTED_DIR)


# # Downloading All NOAA Files


for year in years:

    server = 'ftp.ncdc.noaa.gov'

    def get_all_files_for_year(year):

        YEAR_DIR = os.path.join(BASE_DIR + os.sep + 'FTP' + year)

        if not os.path.exists(YEAR_DIR):

            os.makedirs(YEAR_DIR)

        os.chdir(YEAR_DIR)

        filelocation = 'pub/data/noaa/isd-lite/{}'.format(year)\

        ftp.cwd(filelocation)

        for file in ftp.nlst():

            out = open(file, 'wb')
            ftp.retrbinary("RETR "+ file, out.write)
            out.close()


        os.chdir(BASE_DIR)

    with ftplib.FTP(server, timeout = 1000) as ftp:

        ftp.login()

        get_all_files_for_year(year)


# # Turning files into excels


def getData(year):

    '''
    Takes in a string of the year year and writes an excel file of data from the folder of that year
    '''

    frame_year = pd.DataFrame(index = np.arange(datetime.date(int(year),1,1), datetime.date(int(year)+1,1,1)))

    YEAR_DIR = os.path.join(BASE_DIR + os.sep + 'FTP' + year)

    onlyfiles = [f for f in listdir(YEAR_DIR) if isfile(join(YEAR_DIR, f))]

    for file in onlyfiles:

        try:

            station_id = file.split('-')[0]
            station_country, station_state = getCountry(station_id)
            file_dir = os.path.join(YEAR_DIR + os.sep + file)

            # Making sure the reading is in the CONUS
            if station_country != 'US' or station_state in ['HI','AK']:

                pass

            else:

                station_lat, station_lon = station_lat_long(station_id)
                station_fips = get_fips(station_lat, station_lon)
                opened = gzip.open(file_dir)
                lines = opened.readlines()
                lines = [line.decode('utf-8') for line in lines]

                dates = []
                temperatures = []

                for line in lines:

                    datet = datetime.date(int(line[:5]),int(line[5:8]),int(line[8:11]))
                    temperature = float(line[14:20])/10.0

                    f_temperature = (temperature*9)/5+32

                    dates.append(datet)
                    temperatures.append(f_temperature)

                opened.close()

                for date, temp in zip(dates, temperatures):

                    frame_year.loc[date, station_fips] = temp

        except:

            pass

    os.chdir(EXCELS_DIR)

    writer = ExcelWriter('Data{}.xlsx'.format(year))
    frame_year.to_excel(writer)
    writer.save()

    os.chdir(BASE_DIR)

for year in years:

    getData(year)


# # Reading Excel Files


organizedfiles = ['Data{}.xlsx'.format(year) for year in years]

final_weighted_frame = pd.DataFrame()

for file_name in organizedfiles:

    file_dir = os.path.join(EXCELS_DIR + os.sep + file_name)

    file_frame = pd.read_excel(file_dir)

    # Null readings are replaced with -1767.82 in the excel files
    file_frame.replace(-1767.82, np.nan, inplace=True)

    t = file_frame.T

    test_dict = {}

    for column in t.columns:

        test_col = t[column]

        fips_codes = []
        temperatures = []

        for fips_code, temperature in test_col.iteritems():

            try:

                if math.isnan(float(temperature)) or math.isnan(int(fips_code)) or fips_code not in population_dataframe.index:

                    pass

                else:

                    fips_codes.append(fips_code)
                    temperatures.append(temperature)

            except ValueError:

                pass

        # Getting a list in the same order of fips codes but now its their population
        # Will then turn that into a list of the same length for the weights of every fips code in the same order of the temperatures list

        fips_populations = [population_dataframe.loc[fips]['Population'] for fips in fips_codes]
        total_population = np.sum(fips_populations)
        fips_weights = [float(population/total_population) for population in fips_populations]

        population_weighted_average = np.sum([weight*temp for weight,temp in zip(fips_weights,temperatures)])

        test_dict[column] = population_weighted_average

    yearly_weighted_average_frame = pd.DataFrame(test_dict, index = test_dict.keys())[:1].T

    yearly_weighted_average_frame.columns = ['Weighted Temperature']

    os.chdir(YEARLY_WEIGHTED_DIR)

    writer = ExcelWriter('YearlyWeighted' + file_name)

    yearly_weighted_average_frame.to_excel(writer)

    writer.save()

    os.chdir(BASE_DIR)

final = pd.DataFrame()

for file in organizedfiles:

    file_string = 'YearlyWeighted' + file

    FILE_DIR = os.path.join(YEARLY_WEIGHTED_DIR + os.sep + file_string)

    yearly_frame = pd.read_excel(FILE_DIR)

    final = pd.concat([final, yearly_frame], axis=0)


# # Finding Flat Average


excelfiles = [f for f in listdir(EXCELS_DIR) if isfile(join(EXCELS_DIR, f))]

all_averages = {}

for file in excelfiles:
    year = file.split('Data')[1].split('.')[0]
    FILE_DIR = os.path.join(EXCELS_DIR + os.sep + file)
    file_frame = pd.read_excel(FILE_DIR)
    file_frame.replace(-1767.82, np.nan, inplace=True)
    normal_averages = {row[0]:np.average([r for r in row[1] if not math.isnan(r)]) for row in file_frame.iterrows()}
    all_averages[year] = normal_averages

Averages_frame = pd.DataFrame(columns = ['Flat Average Temperature'])

for year in years:

    new = all_averages.get(year)
    new_frame = pd.DataFrame(new, index = new.keys())[:1].T
    new_frame.columns = ['Flat Average Temperature']
    Averages_frame = pd.concat([Averages_frame, new_frame], axis = 0)

writer = ExcelWriter('FlatAverageTemperatures.xlsx')
Averages_frame.to_excel(writer)
writer.save()


# # Finding Flat vs Weighted


flat_average = pd.read_excel('FlatAverageTemperatures.xlsx')

differences = {}

for date, temperature in flat_average.iterrows():

    try:

        flat_temp = list(temperature)[0]

        weighted_temp = final.loc[date]

        weight_temp = list(weighted_temp)[0]

        difference =  flat_temp - weight_temp

        differences[date] = difference

    except KeyError:

        pass

diff_frame = pd.DataFrame(index = differences.keys())
diff_frame['Difference'] = differences.values()
diff_frame.dropna(inplace=True)

writer = ExcelWriter('Differences.xlsx')
diff_frame.to_excel(writer)
writer.save()
