import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import geoip2.database
import matplotlib.pyplot as plt 

datafile = 'dataset6/data6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
mean = timestamp.mean()
std  = timestamp.std()
minn = timestamp.min()
maxx = timestamp.max()
print("mean timestamp:", mean)
print("std timestamp:", std)
print("min: ", minn)
print("max: ", maxx)

datafile = 'dataset6/servers6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
print("mean timestamp:", (timestamp).mean())
print("std timestamp:", (timestamp).std())
print("min timestamp:", (timestamp).min())
print("max timestamp:", (timestamp).max())

# TODO: what do these values mean????

# weird_timings = data.loc[(data["diff_timestamp"] > maxx) | (data["diff_timestamp"] < minn)] #.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
weird_timings = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
weird_timings = weird_timings.loc[(weird_timings > maxx) | (weird_timings < minn)]
print(weird_timings)
