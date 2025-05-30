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
std = timestamp.std()
print("mean timestamp:", mean)
print("std timestamp:", std)

# TODO: this gives us a 'mean' and 'std' that do not cover the entire range for the 'good' actors
# meaning that we would have false positives (is this accepted?)
# print(timestamp)

datafile = 'dataset6/servers6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
print("mean timestamp:", (timestamp).mean())
print("std timestamp:", (timestamp).std())

weird_timings = data.loc[(data["diff_timestamp"] > (mean + std)) | (data["diff_timestamp"] < (mean - std))]
print(weird_timings)
