from __future__ import print_function
from fredapi import Fred
from datetime import datetime, date, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from configparser import ConfigParser
from data_retrieval import *
import pyodbc
import time
import os

class Data_Update(Data_Retrieval):

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
		scheduler = BackgroundScheduler()
		scheduler.add_job(update_table, 'cron', hour=9, minute=45, misfire_grace_time=3600)
		scheduler.start()

		try:
			while True:
				time.sleep(2)
		except (KeyboardInterrupt, SystemExit):
			scheduler.shutdown()