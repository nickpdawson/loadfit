import pandas as pd
import pygsheets
from influxdb import InfluxDBClient

INFLUXUSER=''
INFLUXPASSWORD=''

#authorize for google sheets
c = pygsheets.authorize()
#select the health metrics worksheet
sh = c.open('Health Metrics')
#define the two different tabs / worksheets
wks = sh.worksheet_by_title("Daily Metrics")
wkssleep = sh.worksheet_by_title("Sleep")


#connect to influxdb
client = InfluxDBClient(host='gondolaone.nsnet.us', port=8086, username=INFLUXUSER, password=INFLUXPASSWORD)
client.switch_database('health')

#get records from googl sheets
records = wks.get_all_records()
sleeprecords = wkssleep.get_all_records()

#read in health metrics sheet
df = pd.DataFrame(records)
#read in sleep data
dfsleep = pd.DataFrame(sleeprecords)

#-------------------
# WORK ON HEALTH METRICS DATA AND LOAD INTO INFLUX
#-------------------

#do conversions of data
#convert date to datetime
df.Date = pd.to_datetime(df.Date)
#remove blanks in vo2 by filling with zeros
#df['VO₂ max'] = df['VO₂ max'].fillna(0)
#fill blanks with zeros
df['VO₂ max'] = df['VO₂ max'].replace('',None)
#remove na in HRV
#df['HRV'] = df['HRV'].fillna(0)
#fill HRV zeros as 0 integers
df['HRV'] = df['HRV'].replace('',None)
#replace al missing data with zeros
df['Resting Energy'] = df['Resting Energy'].replace('',None)
df['Active Energy'] = df['Active Energy'].replace('',None)
df['Steps'] = df['Steps'].replace('',None)
#iterate through health metrics and add to influxdb
for row_index, row in df.iterrows():
  tags = row.name
  datedata = row[0]
  hrvdata = pd.to_numeric(row['HRV'])
  restingdata = pd.to_numeric(row['Resting Energy'])
  activedata = pd.to_numeric(row['Active Energy'])
  stepsdata = row.Steps
  vo2data = pd.to_numeric(row['VO₂ max'])
  json_body = [
    {
        "measurement": "Health Data",
        "tags": {
            "reference": tags
        },
        "time": datedata,
        "fields": {
            "Active Energy": activedata,
            "HRV": hrvdata,
            "Resting": restingdata,
            "Steps": stepsdata,
            "VO2 max": vo2data
        }
    }
  ]
  print(json_body)
  client.write_points(json_body)



#-------------------
# WORK ON SLEEP DATA AND LOAD INTO INFLUX
#-------------------



#make date time strings for Awake data
#Awake = dfsleep.Date + ' ' + dfsleep.Start

#make date time strings out of Start and End Sleep times
dfsleep.Start = pd.to_datetime(dfsleep.Date + ' ' + dfsleep.Start)
dfsleep.End = pd.to_datetime(dfsleep.Date + ' ' + dfsleep.End)

#convert efficency to integer
dfsleep.Efficiency = dfsleep.Efficiency.replace('',0)
dfsleep.Efficiency = dfsleep.Efficiency.str.strip('%')
#.astype(int)

#convert awake time to deltatime
# remove m for minutes
dfsleep['Asleep'] = pd.Series(dfsleep['Asleep']).str.replace('m','')
# remove h for hours
dfsleep['Asleep'] = pd.Series(dfsleep['Asleep']).str.replace('h','')
#convert asleep to timedelta
dfsleep.Asleep = pd.to_timedelta(dfsleep.Asleep + ':' + '00')
#convert Asleep to integer of nanoseconds and store in new series called TimeAsleep
dfsleep['TimeAsleep'] = dfsleep['Asleep'].astype('int')


dfsleep['Awake'] = pd.Series(dfsleep['Awake']).str.replace('m','')
# remove h for hours
dfsleep['Awake'] = pd.Series(dfsleep['Awake']).str.replace('h','')
#for now, break into array THIS IS IT
dfsleep.Awake = pd.to_timedelta(dfsleep.Awake + ':' + '00')
#convert Awake to integer of nanoseconds and store in new series called TimeAwake
dfsleep['TimeAwake'] = dfsleep['Awake'].astype('int')


dfsleep['InBed'] = pd.Series(dfsleep['InBed']).str.replace('m','')
# remove h for hours
dfsleep['InBed'] = pd.Series(dfsleep['InBed']).str.replace('h','')
#for now, break into array THIS IS IT
dfsleep.InBed = pd.to_timedelta(dfsleep.InBed + ':' + '00')
#convert time in bed to integer of nanoseconds and store in new series called TimeInBed
dfsleep['TimeInBed'] = dfsleep['InBed'].astype('int')

#To be done
#dfsleep['Fall Asleep'] = pd.Series(dfsleep['Fall Asleep']).str.replace('m','')
# remove h for hours
#dfsleep['Fall Asleep'] = pd.Series(dfsleep['Fall Asleep']).str.replace('h','')
#convert to datetime
#dfsleep['Fall Asleep'] = pd.to_timedelta(dfsleep['Fall Asleep'] + ':' + '00')
#convert time to fall asleep to integer of nanoseconds and store in new series called TimeInBed
#dfsleep['TimeFallAsleep'] = dfsleep['Fall Asleep'].astype('int')

#convert to datetime format DO THIS LAST
dfsleep.Date = pd.to_datetime(dfsleep.Date)

#iterate through sleep data and add to influxdb
for sleep_index, sleeprow in dfsleep.iterrows():
  sleeptags = sleeprow.name
  asleepdata = sleeprow.TimeAsleep
  awakedata = sleeprow['TimeAwake']
  sleepdate = sleeprow[0]
  sleepefficiency = sleeprow['Efficiency']
  wakeup = sleeprow['End']
  #fallasleep = sleeprow['Fall Asleep']
  inbed = sleeprow['TimeInBed']
  maingoal = sleeprow['Main']
  sleepstart = sleeprow['Start']
  wakecount = sleeprow['Wake Count']
  sleepjson_body = [
    {
        "measurement": "Sleep Data",
        "tags": {
            "reference": sleeptags
        },
        "time": sleepdate,
        "fields": {
            "Asleep": asleepdata,
            "Time Spent Awake": awakedata,
            "Sleep Efficiency": sleepefficiency,
            #"Wake up": wakeup
            #"Time to Fall Asleep": fallasleep,
            "In bed": inbed,
            "Goal": maingoal,
            #"Went to bed": sleepstart,
            "wake count": wakecount
        }
    }
  ]
  print(sleepjson_body)
  client.write_points(sleepjson_body)
