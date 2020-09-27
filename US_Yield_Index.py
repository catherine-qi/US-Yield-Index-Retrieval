#Catherine Qi
#6/10/20

from __future__ import print_function
from fredapi import Fred
import pyodbc

from datetime import datetime, date, timedelta
import time
import os
from apscheduler.schedulers.background import BackgroundScheduler

class US_Yield_Index:

	global fred_api_key
	global fred
	global data_ID
	global server
	global database
	global yesterday
	global connection
	global cursor

	fred_api_key = '8c40772b4bd4fb7e3cb81d51799735d3'

	fred = Fred(api_key=fred_api_key)

	#BAMLH0A0HYM2: is ICE BofA US High Yield Index
	#BAMLH0A3HYC: is ICE BofA Lower US High Yield Index
	data_ID = ['BAMLH0A0HYM2', 'BAMLH0A3HYC']

	server = 'LAPTOP-THCNHFOF\SQLEXPRESS'
	database = 'stock_database'

	yesterday = str(date.today() - timedelta(days=1))

	connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
								SERVER=' + server + ';\
								DATABASE=' + database +';\
								Trusted_Connection=yes;')

	#cursor connection
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

	#scheduler
	def update_table():
		yesterday = str(date.today() - timedelta(days=1))#getting new date

		for i in range(len(data_ID)):
			new_data = fred.get_series(data_ID[i], observation_start=yesterday, observation_end=yesterday)#retrieving new data

			if (new_data.index.values.size == 0):
				continue

			#new date
			new_data_date = str(new_data.index.values[0])
			new_data_date = new_data_date[0:10]

			#new moving average
			#data = fred.get_series(data_ID[i], observation_start='1996-12-31', observation_end=yesterday)
			#new_data_moving = data.rolling(window=5).mean().values[len(data) - 1]
			#new_data_moving = str(round(new_data_moving,3))

			#new standard deviation
			#new_data_deviation = data.rolling(window=144,min_periods=1).std().values[len(data)-1]
			#new_data_deviation = str(round(new_data_deviation,3))

			#inserting new values
			if(str(new_data.values[0]) != 'nan'):
				insert_query = 'INSERT INTO US_High_Yield_Index (value_date, value, data_type) VALUES(\'' + new_data_date + '\',\''  + str(new_data.values[0]) + '\',\'' + data_ID[i] + '\');'
				print(insert_query)

				cursor.execute(insert_query)

		connection.commit()

	if __name__ == "__main__":
		retrieveData()

		scheduler = BackgroundScheduler()
		scheduler.add_job(update_table, 'cron', hour=9, minute=45)
		scheduler.start()

		try:
			while True:
				time.sleep(2)
		except (KeyboardInterrupt, SystemExit):
			scheduler.shutdown()

		cursor.close()
		connection.close()