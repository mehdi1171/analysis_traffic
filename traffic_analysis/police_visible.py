import psycopg2
from pandas import DataFrame, to_datetime
from geopandas import GeoDataFrame, GeoSeries
import datetime
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates
from datetime import datetime
from sklearn.cluster import DBSCAN


def convert_date_month(val):
    year_month_var = datetime.strptime(val, '%Y-%m')
    month_val = datetime.strftime(year_month_var, '%B')
    return month_val


def year_month(val):
    return val.strftime('%Y-%m')


def sparsity_of_data(base_df):
    date_value = (base_df["Dates"].value_counts())
    x = list(date_value.index)
    y = list(date_value.values)
    time_series_df = DataFrame({"Date": x, "data_counts": y})
    time_series_df["Date"] = time_series_df["Date"].map(year_month)
    time_series_df = time_series_df.groupby("Date")[["data_counts"]].sum()
    time_series_df = time_series_df.reset_index(inplace=True)
    time_series_df = time_series_df.sort_values(by=["Date"])
    time_series_df["Month"] = time_series_df["Date"].map(convert_date_month)
    # plot
    plt.stem(time_series_df["Month"], time_series_df["data_counts"])
    plt.title('Police Visibility Data Sparsity')
    plt.xlabel('Date')
    plt.ylabel('Data Frequency')
    plt.show()


""" Initialize"""
#  Connect to db:
connection = psycopg2.connect(
    host="localhost",
    database="waze_db",
    user="postgres",
    password="1171")
my_cursor = connection.cursor()
# Query:
sql_query = """ select geometry, "reportRating", "reportMood", "reportDescription", reliability, confidence, "type", subtype, "roadType", street, "time", routaa_type_id 
from waze_alerts_with_id wawi 
where wawi.routaa_type_id = '45' """

df = GeoDataFrame.from_postgis(sql_query, connection, geom_col='geometry', crs=3857)
""" Separate date time """
df['Date'] = to_datetime(df['time']).dt.date
df['Time'] = to_datetime(df['time']).dt.time

# df.sort_values(by=["Dates"])
""" check geometry data """
# geom_type = list(df.geometry.type)
# for g in geom_type:
#     if g != 'Point':
#         print('we have another type of geometry')
""" Export geojson file """
# df.to_file('police_visible.geojson', driver='GeoJSON')

""" Police Visibility Data Sparsity """
# sparsity_of_data(df)
""" pre-processing """
data = df
times = list(set(list(data["Date"])))
selected_df = data[data["Date"] == times[0]]

selected_df.to_file('first_day.geojson', driver='GeoJSON')
""" Clustering """
data = df
times = list(set(list(data["Dates"])))
geometry_series = data["geometry"]
coord_data = []
for row in geometry_series:
    long, lat = row.coords.xy
    coord_data.append([list(long)[0], list(lat)[0]])

clustering = DBSCAN(eps=25, min_samples=5).fit(np.array(coord_data))

cluster_label = clustering.labels_
data["cluster_label"] = cluster_label
# data["cluster_label"].value_counts()
export_df = df[df["cluster_label"]==-1]

export_df.to_file('outlier.geojson', driver='GeoJSON')
