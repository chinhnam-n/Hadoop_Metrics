import requests
import pandas as pd
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from getpass import getpass

    #
    # MAKE SURE TO ADJUST SAVE LOCATION
    #

def getCall(url, username, password):
    response = requests.get(url, verify=False, auth=HTTPBasicAuth(username, password))
    data = response.json()  # stores api response as proper json format
    return data


def getDates(delta):  # gets current date and desired delta date for api time series query
    # gets current date and formats to api needs
    current_date = datetime.utcnow()
    current_date = str(current_date).replace(" ", "T")
    current_date = current_date.replace(":", "%3A")

    # gets current date minus delta days and formats to api needs
    delta_date = datetime.utcnow() - timedelta(days=delta)
    delta_date = str(delta_date).replace(" ", "T")
    delta_date = delta_date.replace(":", "%3A")

    return current_date, delta_date


def createHdfsPerUser(username, password):
    url = '.../api/v42/clusters/TCDH1Sa/services/hdfs/reports/hdfsUsageReport'  # url that returns json data for hdfs usage of each user
    entityDf = pd.DataFrame()  # creates empty dataframe
    data = getCall(url, username, password)
    entityDf = pd.json_normalize(data, record_path=['items'], meta=['lastUpdateTime'])  # flattens out json data and stores as dataframe

    with open('.../hdfs_usage_per_user.csv', 'w') as f:
        entityDf.to_csv(f, header=True, index=False)  # converts dataframe to csv, removes index, writes to local csv file

    # removes first empty row in the csv
    with open(".../hdfs_usage_per_user.csv",'r') as f:
        with open('.../formatted_hdfs_usage_per_user.csv','w') as f1:
            next(f)  # skip header line
            for line in f:
                f1.write(line)


def createHdfsUsage(username, password): # creates timeseries report for total hdfs capacity used for a given time period
    delta = 1 # Number of days back that the time series is queried
    date_interval = getDates(delta)
    to_date = date_interval[0]
    from_date = date_interval[1]
    url = '.../api/v6/timeseries?query=select%20dfs_capacity_used%20where%20entityName%3D%22hdfs%3Atcdh1sa%22&contentType=application%2Fjson&desiredRollup=HOURLY&from=' + from_date + '&to=' + to_date
    entityDf = pd.DataFrame() #creates empty dataframe
    data = getCall(url, username, password)
    #extracts relevant data of nested json being returned
    items = data['items']
    for i in items:
        if 'timeSeries' in i:
            timeSeries = i['timeSeries']
            for t in timeSeries:
                if 'data' in t:
                    data = t['data']
    entityDf = pd.json_normalize(data)
    with open('.../total_hdfs_usage.csv', 'w') as f:
        entityDf.to_csv(f, header=True, index=False) #converts dataframe to csv, removes index, writes to local csv file

    # removes first empty row in the csv
    with open(".../total_hdfs_usage.csv",'r') as f:
        with open('.../formatted_total_hdfs_usage.csv','w') as f1:
            next(f)  # skip header line
            for line in f:
                f1.write(line)


def createCpuUsage(username, password):
    delta = 1  # Number of days back that the time series is queried
    date_interval = getDates(delta)
    to_date = date_interval[0]
    from_date = date_interval[1]
    url = 'https://.../api/v6/timeseries?query=select%20cpu_percent_across_hosts%20where%20category%20%3D%20CLUSTER%20and%20clusterId%20%3D%20%221546335356%22&contentType=application%2Fjson&desiredRollup=HOURLY&from=' + from_date + '&to=' + to_date
    entityDf = pd.DataFrame()  # creates empty dataframe
    data = getCall(url, username, password)
    items = data['items']
    for i in items:
        if 'timeSeries' in i:
            timeSeries = i['timeSeries']
            for t in timeSeries:
                if 'data' in t:
                    data = t['data']
    entityDf = pd.json_normalize(data)
    with open('.../total_cpu_usage.csv', 'w') as f:
        entityDf.to_csv(f, header=True, index=False)  # converts dataframe to csv, removes index, writes to local csv file

    # removes first empty row in the csv
    with open(".../total_cpu_usage.csv",'r') as f:
        with open('.../formatted_total_cpu_usage.csv','w') as f1:
            next(f)  # skip header line
            for line in f:
                f1.write(line)


username = input("Enter your username: ")
password = getpass('Enter your password:')

createHdfsPerUser(username, password)
createHdfsUsage(username, password)
createCpuUsage(username, password)