import pandas as pd

datafile = 'dataset6/data6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
mean = timestamp.mean()
std  = timestamp.std()
minn = timestamp.min()
maxx = timestamp.max()
print("mean timestamp (data):", mean)
print("std timestamp (data):", std)
print("min timestamp (data): ", minn)
print("max timestamp (data): ", maxx)

datafile = 'dataset6/servers6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
print("\nmean timestamp (servers):", (timestamp).mean())
print("std timestamp (servers):", (timestamp).std())
print("min timestamp (servers):", (timestamp).min())
print("max timestamp (servers):", (timestamp).max())

# TODO: what do these values mean, like is 38 seconds a lot? And how much is too low?

weird_timings = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
weird_timings = weird_timings.loc[(weird_timings > maxx) | (weird_timings < minn)]
print(weird_timings)
