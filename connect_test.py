import psycopg
from psycopg import sql
from psycopg.types.composite import CompositeInfo, register_composite
import argparse
from datetime import datetime, timezone, timedelta
import random
import math

parser = argparse.ArgumentParser(description='Connect to test PODA')
parser.add_argument('-pass', help='PGPASSWORD', dest='password', required=True)
parser.add_argument('-host', help='PGHOST', dest='host', required=True)
parser.add_argument('-db', help='Database to connect to', dest='db', required=True)
parser.add_argument('-user', help='User to connect with', dest='user', required=True)
args = parser.parse_args()

connect_string = "dbname='%s' user='%s' host='%s' password='%s'" % (args.db, args.user, args.host, args.password)
print(connect_string)

date_format = '%Y-%m-%d %H:%M:%S'

try:
    conn = psycopg.connect(connect_string)
except:
    print("I am unable to connect to the database \n")

cur = conn.cursor()

# make sure we have the schema(s)
with open("db/public.sql") as file:
    public = file.read()
#print("inserting schema: \n", public) 
cur.execute(public)

with open("db/labels.sql") as file:
    labels = file.read()
cur.execute(labels)

# get the location type
location = CompositeInfo.fetch(conn, "location")
register_composite(location, conn)

# get the filterlabel type
filterlabel = CompositeInfo.fetch(conn, "filterlabel")
register_composite(filterlabel, conn)

# create a bunch of timeseries
for x in range(0, 10):
    now = datetime.now(timezone.utc)
    random_past_date = (now - timedelta(days=(random.randrange(365)))).strftime("%Y-%m-%d")
    # random lat / lon somewhere in the vicinity of norway
    lat = random.randrange(59, 72)*(0.5+random.random())
    lon = random.randrange(4, 30)*(0.5+random.random())
    hamsl = float(random.randint(0,1000))
    my_loc = location.python_type(lat, lon, hamsl, 0)
    cur.execute("INSERT INTO timeseries (fromtime, loc, deactivated) VALUES(%s, %s, false)", (random_past_date, my_loc)) 

# create the actual partitions
for year in range(2020,2024):
    for month in range(1,13):
        if month < 10:
            start = str(year) + "-0" + str(month) + "-" + "01" 
            end = str(year) + "-0" + str(month+1) + "-" + "01"
        if month == 9:
            start = str(year) + "-0" + str(month) + "-" + "01" 
            end = str(year) + "-" + str(month+1) + "-" + "01"
        if month == 12: 
            start = str(year) + "-" + str(month) + "-" + "01" 
            end = str(year+1) + "-0" + str(1) + "-" + "01"
        else: 
            start = str(year) + "-" + str(month) + "-" + "01"
            end = str(year) + "-" + str(month+1) + "-" + "01"  
        name = "data_y" + str(year) + "m" + str(month)
        print("partion: ", name, " ", start, " ", end)
        cur.execute(sql.SQL("CREATE TABLE {} PARTITION OF data FOR VALUES FROM ({}) TO ({})").format(sql.Identifier(name), start, end))

# check what has been inserted
cur.execute('SELECT * from timeseries;') 
contents_of_timeseries = cur.fetchall()

# list of elements
elem_list = ["air_temperature", "precipitation", "wind_speed"]

# then make some fake random data
### seems like it always starts the serial at 1?
for x in contents_of_timeseries:
    # label the timeseries 
    stn = random.randrange(1000, 2000)
    elem = random.choice(elem_list)
    label = filterlabel.python_type(stn, elem, 0, 0)
    cur.execute("INSERT INTO labels.filter (timeseries, label) VALUES(%s, %s)", (x[0], label)) 
    # make some hourly data
    # starting from the day it says it starts until now?
    print(x)
    # [1] is the fromtime
    delta = datetime.now(timezone.utc) - x[1]
    time_d = math.floor(delta.days)
    # loop over the days
    for days in range(0,time_d):
        # loop over hours in the day
        for hour in range(0,24):
            # random data value
            rand_data = random.random()*10
            # construct timestamp
            timeS = x[1] + timedelta(days=(days), hours=(hour))
            #print(x[0], " ", timeS, " ", rand_data)
            # [0] is the serial
            cur.execute("INSERT INTO data (timeseries, timestamp, value) VALUES(%s, %s, %s)", (x[0], timeS, rand_data)) 

# Make the changes to the database persistent
conn.commit()

cur.execute('SELECT COUNT(timeseries) FROM data')
result = cur.fetchone()
print(result)

cur.close()
conn.close()