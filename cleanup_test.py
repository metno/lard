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

# detach the partitions
for year in range(2020,2024):
    for month in range(1,13):
        name = "data_y" + str(year) + "m" + str(month)
        #cur.execute(sql.SQL("ALTER TABLE data DETACH PARTITION {}").format(sql.Identifier(name)))
        #cur.execute(sql.SQL('TRUNCATE {}').format(sql.Identifier(name)))
        #cur.execute(sql.SQL('DROP TABLE {}').format(sql.Identifier(name)))

# cleanup
#cur.execute('TRUNCATE timeseries, data, filter;')
cur.execute('DROP TABLE timeseries, data, labels.filter CASCADE;')
cur.execute('DROP TYPE location, filterlabel;')

# Make the changes to the database persistent
conn.commit()

cur.close()
conn.close()