import psycopg2
from pandas import DataFrame, to_datetime
from geopandas import GeoDataFrame, GeoSeries
import datetime

connection = psycopg2.connect(
    host="localhost",
    database="waze_db",
    user="postgres",
    password="1171")
my_cursor = connection.cursor()

sql_query = """ select geometry, "reportRating", "reportMood", "reportDescription", reliability, confidence, "type", subtype, "roadType", street, "time", routaa_type_id 
from waze_alerts_with_id wawi 
where wawi.routaa_type_id = '50' """

df = GeoDataFrame.from_postgis(sql_query, connection, geom_col='geometry')

geom_type = list(df.geometry.type)
for g in geom_type:
    if g != 'Point':
        print('we have another type of geometry')

df.to_file('waze_jam_traffic.geojson', driver='GeoJSON')

df['Dates'] = to_datetime(df['time']).dt.date
df['Time'] = to_datetime(df['time']).dt.time
df.drop(['time'], axis=1, inplace=True)

df.sort_values(by=["Dates"])
days = list(set(list(df["Time"])))
print(days)
print(len(days))
