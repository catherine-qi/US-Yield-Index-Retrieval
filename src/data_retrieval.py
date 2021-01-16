from __future__ import print_function
from fredapi import Fred
from datetime import datetime, date, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from configparser import ConfigParser
import pyodbc
pyodbc.__version__
import time
import os

class Data_Retrieval: 

	global fred
	global data_ID
	global yesterday
	global connection
	global cursor

	#config variables
	config = ConfigParser()
	config.read('config.ini')
	fred_api_key = config['FRED']['fred_api_key']
	server = config['USER']['server']
	database = config['USER']['database']

	#other class variables
	fred = Fred(api_key=fred_api_key)
	#BAMLH0A0HYM2: is ICE BofA US High Yield Index
	#BAMLH0A3HYC: is ICE BofA Lower US High Yield Index
	data_ID = ['BAMLH0A0HYM2', 'BAMLH0A3HYC']
	yesterday = str(date.today() - timedelta(days=1))
	connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
								SERVER=' + server + ';\
								DATABASE=' + database +';\
								Trusted_Connection=yes;')
	cursor = connection.cursor()

	#retrieving data from FRED
	def retrieveData():
		for i in range(len(data_ID)):
			data = fred.get_series(data_ID[i], observation_start='1996-12-31', observation_end=yesterday)
			values_array = []

		#creating the 2D array, values_array
		for j in range(len(data.index.values)):
			#getting dates (value_date)
			value_date = str(data.index.values[j])
			value_date = value_date[0:10]

			#getting moving average (data_moving)
			#data_moving = data.rolling(window=5).mean().values[j]
			#data_moving = str(round(data_moving,3))

			#standard deviation
			#data_deviation = data.rolling(window=144,min_periods=1).std().values[j]
			#data_deviation = str(round(data_deviation,3))

			#[date, value,]
			values_array.append([value_date, str(data.values[j])])

		#looping through array to insert rows
		for index, row in enumerate(values_array):
			if (row[1] == 'nan'):
				continue

			#if (row[2] == 'nan'):
				#row[2] = '0'b

			#if (row[3] == 'nan'):
				#row[3] = '0'

			insert_query = 'INSERT INTO US_High_Yield_Index (value_date, value, data_type) VALUES(\'' + row[0] + '\',\''  + row[1] + '\',\'' + data_ID[i] + '\');'
			print(insert_query)

			cursor.execute(insert_query)

		connection.commit()

	if __name__ == "__main__":
		retrieveData()


