import os
import pickle
import pandas as pd
import pmdarima as pm
from pathlib import Path
from shutil  import rmtree
from pymongo import MongoClient
from sklearn.ensemble import RandomForestRegressor


#######################################################################
#                                                                     #
# PROCESS DATA                                                        #
#   1) Extract DATETIME and SAN FRANCISCO columns from humidity.csv   #
#   and temperature.csv.                                              #
#   2) Create a new dataset with the following columns:               #
#       - DATE - intersection of the DATETIME columns from two both   #
#   datasets.                                                         #
#       - TEMP - SAN FRANCISCO column from temperature.csv.           #
#       - HUM  - SAN FRANCISCO column from humidity.csv.              #
#   3) Store the new dataset in MongoDB.                              #
#                                                                     #
#######################################################################
def selectCSVcolumns(csvfile, column_name):
    """
        Extracts specific columns from a CSV file.
    """
    data = pd.read_csv(csvfile, usecols=['datetime','San Francisco'])
    return data.rename(columns={'datetime':'DATE', 'San Francisco': column_name})


def mergeDataSets(hum_file, temp_file):
    """
        Merges datasets with a common datetime column and
      stores the new dataset in MongoDB.
    """
    dataA = selectCSVcolumns(hum_file, 'HUM')
    dataB = selectCSVcolumns(temp_file, 'TEMP')
    data = dataB.merge(dataA, on='DATE')
    data = data.dropna()

    # Connect to MongoDB
    client = MongoClient('localhost', 27017)

    # Get hum_temp collection of the database
    hum_temp = client.database.hum_temp

    # Store the data in the database
    hum_temp.insert_one({'index' : 'Humidity-Temperature',
                         'data' : data.to_dict('records')})

    client.close()


#######################################################################
#                                                                     #
# CREATE ARIMA MODEL                                                  #
#   1) Extract dataset from MongoDB.                                  #
#   2) Train with the humidity and temperature sets.                  #
#   3) Stores the ARIMA models in pickle files.                       #
#                                                                     #
#######################################################################
def trainARIMA(path):
    """
        Creates ARIMA models (Humidity - Temperature).
    """
    # Connect to MongoDB
    client = MongoClient('localhost', 27017)

    # Get hum_temp collection of the database
    hum_temp = client.database.hum_temp

    # Extract the data from the database
    data_from_db = hum_temp.find_one({'index':'Humidity-Temperature'})
    client.close()

    # Train with a subset
    data = pd.DataFrame(data_from_db['data'])
    data = data.sample(n=1000)

    # Humidity
    model_hum = pm.auto_arima(data['HUM'], start_p=1, start_q=1,
                              test='adf',       # use adftest to find optimal 'd'
                              max_p=3, max_q=3, # maximum p and q
                              m=1,              # frequency of series
                              d=None,           # let model determine 'd'
                              seasonal=False,   # No Seasonality
                              start_P=0,
                              D=0,
                              trace=True,
                              error_action='ignore',
                              suppress_warnings=True,
                              stepwise=True)
    # Temperature
    model_temp = pm.auto_arima(data['TEMP'], start_p=1, start_q=1,
                               test='adf',       # use adftest to find optimal 'd'
                               max_p=3, max_q=3, # maximum p and q
                               m=1,              # frequency of series
                               d=None,           # let model determine 'd'
                               seasonal=False,   # No Seasonality
                               start_P=0,
                               D=0,
                               trace=True,
                               error_action='ignore',
                               suppress_warnings=True,
                               stepwise=True)

    # Store models
    Path(path).mkdir(parents=True, exist_ok=True)
    pickle.dump(model_hum,  open(path+'/arima_humidity.p', 'wb'))
    pickle.dump(model_temp, open(path+'/arima_temperature.p', 'wb'))


#######################################################################
#                                                                     #
# CREATE RANDOM FOREST MODEL                                          #
#   1) Extract dataset from MongoDB.                                  #
#   2) Train with the humidity and temperature sets.                  #
#   3) Stores the Random Forest models with pickle.                   #
#                                                                     #
#######################################################################
def trainRandomForest(path):
    """
        Creates Random Forest models (Humidity - Temperature).
    """
    # Connect to MongoDB
    client = MongoClient('localhost', 27017)

    # Get hum_temp collection of the database
    hum_temp = client.database.hum_temp

    # Extract the data from the database
    data_from_db = hum_temp.find_one({'index':'Humidity-Temperature'})
    client.close()

    # Train with a subset
    data = pd.DataFrame(data_from_db['data'])
    data = data.sample(n=1000)

    # Training input samples: list of (year, month, day, hour)
    #  -> (NOTE) DATE column format: "%Y-%m-%d %H:%M:%S"
    X = [(datetime[:4], datetime[5:7], datetime[8:10], datetime[11:13])
         for datetime in data['DATE']]

    # Humidity
    model_hum = RandomForestRegressor(max_depth=50,
                                      n_jobs=-1).fit(X, data['HUM'])
    # Temperature
    model_temp = RandomForestRegressor(max_depth=50,
                                      n_jobs=-1).fit(X, data['TEMP'])

    # Store models
    Path(path).mkdir(parents=True, exist_ok=True)
    pickle.dump(model_hum,  open(path+'/rf_humidity.p', 'wb'))
    pickle.dump(model_temp, open(path+'/rf_temperature.p', 'wb'))
