import psycopg
from psycopg.types.composite import CompositeInfo, register_composite
import argparse
from datetime import datetime, timezone, timedelta
import random

parser = argparse.ArgumentParser(description='Connect to test PODA')
parser.add_argument('-pass', help='PGPASSWORD', dest='password', required=True)
parser.add_argument('-host', help='PGHOST', dest='host', required=True)
parser.add_argument('-db', help='Database to connect to', dest='db', required=True)
parser.add_argument('-user', help='User to connect with', dest='user', required=True)
args = parser.parse_args()

connect_string = "dbname='%s' user='%s' host='%s' password='%s'" % (args.db, args.user, args.host, args.password)
print(connect_string)

try:
    conn = psycopg.connect(connect_string)
except:
    print("I am unable to connect to the database \n")

cur = conn.cursor()

# make sure we have the schema
with open("db/schema.sql") as file:
    data = file.read()
#print("inserting schema: \n", data) 
cur.execute(data)

# get the location type
location = CompositeInfo.fetch(conn, "location")
register_composite(location, conn)

# create a bunch of timeseries
for x in range(0, 10):
    now = datetime.now(timezone.utc)
    random_past_date = (now - timedelta(days=(random.randrange(5000)))).strftime("%Y-%m-%d %H:%M:%S")
    dec_lat = random.random()/100
    dec_lon = random.random()/100
    hamsl = float(random.randint(0,1000))
    my_loc = location.python_type(dec_lat, dec_lon, hamsl, 0)
    cur.execute("INSERT INTO timeseries (fromtime, loc, deactivated) VALUES(%s, %s, false)", (random_past_date, my_loc)) 

# check what has been inserted
cur.execute('SELECT * from timeseries;') 
contents_of_timeseries = cur.fetchall()
print(contents_of_timeseries)

# then make some fake random data


# for testing maybe its best to clean up everything after?
cur.execute('TRUNCATE timeseries, data;')

cur.close()
conn.close()