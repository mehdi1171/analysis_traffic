import psycopg2
from pandas import DataFrame, to_datetime, concat
from geopandas import GeoDataFrame, GeoSeries
import datetime
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates
from datetime import datetime
from sklearn.cluster import DBSCAN
from collections import Counter
import requests


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


def get_centroid(df_select):
    long_bar = np.mean(np.array(df_select["long"]))
    lat_bar = np.mean(np.array(df_select["lat"]))
    centroid_coord = [long_bar, lat_bar]
    return centroid_coord


""" Initialize"""
#  Connect to db:
connection = psycopg2.connect(
    host="localhost",
    database="waze_db",
    user="postgres",
    password="1171")
my_cursor = connection.cursor()
# Query:
sql_query = """ select ST_Transform(geometry, 3857) geometry, "reportRating", "reportMood", "reportDescription", reliability, confidence, "type", subtype, "roadType", street, "time", routaa_type_id 
from waze_alerts_with_id wawi 
where wawi.routaa_type_id = '45' """

df = GeoDataFrame.from_postgis(sql_query, connection, geom_col='geometry')
""" Separate date time """
# df['Date'] = to_datetime(df['time']).dt.date
# df['Time'] = to_datetime(df['time']).dt.time

# df.sort_values(by=["Dates"])
""" check geometry data """
# geom_type = list(df.geometry.type)
# for g in geom_type:
#     if g != 'Point':
#         print('we have another type of geometry')
""" Export geojson file """
# df.to_file('new_police_visible.geojson', driver='GeoJSON')

""" Police Visibility Data Sparsity """
# sparsity_of_data(df)
""" pre-processing """
# data = df
# data["num_record"] = data["geometry"].value_counts()
# times = list(set(list(data["Date"])))
# selected_df = data[data["Date"] == times[0]]
# selected_df.drop(["Date", "Time"], axis=1, inplace=True)
# selected_df.to_file('first_day.geojson', driver='GeoJSON')
""" Pre-processing """
data = df
# times = list(set(list(data["Date"])))
geometry_series = data["geometry"]
coord_data = []
for row in geometry_series:
    long, lat = row.coords.xy
    coord_data.append([list(long)[0], list(lat)[0]])
np_cord = np.array(coord_data)
data["x"] = np_cord[:, 0]
data["y"] = np_cord[:, 1]
""" Clustering """
clustering = DBSCAN(eps=30, min_samples=10).fit(np.array(coord_data))

cluster_label = clustering.labels_
num_of_cluster = Counter(cluster_label)
data["cluster_label"] = cluster_label

""" convert geom to 4326 """
df_data = data
df_geom = df_data[["geometry", "cluster_label", "time"]]
df_geom = df_geom.to_crs(4326)


""" Select high confidence point """
count_label = (df_geom["cluster_label"].value_counts())
df_count = DataFrame({"label": count_label.index, "count": count_label.array})
# remove outlier
df_count = df_count[1::]
median_df = np.percentile(df_count["count"], 75)       # > Q3
df_high_conf = df_count[df_count["count"] > 100]
label_with_high_conf = list(df_high_conf["label"])
df_geom_conf = df_geom
df_geom_conf = df_geom_conf[df_geom_conf["cluster_label"].isin(label_with_high_conf)]
df_conf = df_geom_conf.drop_duplicates(subset=['geometry'])

""" Find Cluster Centroid """
df_c = df_conf
long_lat = []
geom_ = df_c["geometry"]
for row in geom_:
    long, lat = row.coords.xy
    long_lat.append([list(long)[0], list(lat)[0]])
np_cord = np.array(long_lat)
df_c["long_lat"] = long_lat
df_c["long"] = np_cord[:, 0]
df_c["lat"] = np_cord[:, 1]
df_centroid = GeoDataFrame()
i = 0
for index in label_with_high_conf:
    i += 1
    print(i)
    cluster_i = df_c[df_c["cluster_label"] == index]
    cord = get_centroid(cluster_i)
    repeat_cord = list([cord] * len(cluster_i))
    cluster_i["coord_mean"] = repeat_cord
    df_centroid = df_centroid.append(cluster_i)
center = df_centroid.drop_duplicates(subset="cluster_label")

""" send request in POST mode """
url = 'http://192.168.7.16:8084/api/pub/feedbacks'
header = {'Content-Type': 'application/json'}
police_loc = list(center["coord_mean"])
for loc in police_loc:
    obj = {
        "lat": loc[1],
        "lng": loc[0],
        "wayId": "0",
        "textComment": "اینجا پلیس حضور دارد",
        "voteType": "POSITIVE",
        "type": "POLICE"
    }
    resp = requests.post(url, json=obj, headers=header)
    print(resp)

""" Export Outlier """
outlier_df = df[df["cluster_label"] == -1]
outlier_df.drop(["Date", "Time"], axis=1, inplace=True)
outlier_df.to_file('outlier.geojson', driver='GeoJSON')
""" Export clusters """
cluster_df = df[df["cluster_label"] != -1]
cluster_df.drop(["Date", "Time"], axis=1, inplace=True)
cluster_df.to_file('cluster_layer.geojson', driver='GeoJSON')


# np_centroid = np.empty(shape=[0, 7])
# i = 0
# for index in label_with_high_conf:
#     i += 1
#     print(i)
#     cluster_i = df_c[df_c["cluster_label"] == index]
#     cord = get_centroid(cluster_i)
#     repeat_cord = list([cord] * len(cluster_i))
#     cluster_i["coord_mean"] = repeat_cord
#     np_cluster = np.array(cluster_i)
#     np_centroid = np.concatenate((np_centroid, np_cluster), axis=0)
