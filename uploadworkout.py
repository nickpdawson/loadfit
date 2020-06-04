import activityio as aio
import pandas as pd
import re
from influxdb import InfluxDBClient
import os
import time

influxhost = ' '
influxuser = ' ' 
influxpass = ' ' 
influxdatabase

#connect to InfluxDB
client = InfluxDBClient(host=influxhost, port=8086, username=influxuser, password=influxpass)
client.switch_database('influxdatabase')

#path to directory with .fit files 
directory = ''

for filename in os.listdir(directory):
    if filename.endswith(".fit"):
      #pull the date from the filename
      pattern = re.compile(r"\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])*")
      worknamepattern = re.compile(r"\bRunning\b")
      #store the resulting date as result[0]
      result = pattern.match(filename)
      workouttype = worknamepattern.match(filename)
      #read in file to adataframe
      data = aio.read(directory + filename)
      #make a datetime using the filename date and the running time of the workout
      data['datetime'] = pd.to_datetime(result[0] + ' ' + data.time.astype('str')  )
      #fill null values for Influxdb
      data = data.fillna(0)
      for row_index, row in data.iterrows():
          tags = row.name
          datedata = row.datetime
          if 'temp' in row: temp = row.temp
          else: temp = 0.0
          if 'cad' in row: cadence = row.cad
          else: cadence = 0.0
          if 'alt' in row: alt = row.alt
          else: alt = 0.0
          if 'hr' in row: hr = row.hr
          else: hr = 0.0
          if 'lon' in row: lon = row.lon
          else: lon = 0.0
          if 'lat' in row: lat = row.lat
          else: lat = 0.0
          if 'distance' in row: distance = row.dist
          else: distance = 0.0
          if 'speed' in row: speed = row.speed
          else: speed = 0.0
          json_body = [
            {
                "measurement": "Workout",
                "tags": {
                    "reference": tags
                },
                "time": datedata,
                "fields": {
                    "Temperature": temp,
                    "Cadence": cadence,
                    "Altitude": alt,
                    "Heart Rate": hr,
                    "Longitude": lon,
                    "Latitude": lat,
                    "Distance": distance,
                    "Speed": speed
                }
            }
          ]
          print(json_body)
          client.write_points(json_body)
          #Slow down for INFLUX
      os.rename(directory + filename, directory+ filename + '_scanned')
      time.sleep(10)
