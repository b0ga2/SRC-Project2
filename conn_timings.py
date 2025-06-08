import pandas as pd

datafile = 'dataset6/servers6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp']

mean = timestamp.mean().to_frame(name='mean')
std  = timestamp.std().to_frame(name='std')
minn = timestamp.min().to_frame(name='min')
maxx = timestamp.max().to_frame(name='max')

FUCK = pd.concat([mean, std, minn, maxx], axis=1).sort_values(by="std", ascending=False)
print(FUCK)
print(timestamp.mean().std())
print(std.loc[std["std"] < timestamp.mean().std()])
print(maxx.loc[maxx["max"] < timestamp.mean().max()])
print("klsaljdklsajdklsajdklsajdkl")
print(timestamp.count().sort_values(ascending=False))

# print("\nmean timestamp (servers):", (timestamp).mean())
# print("std timestamp (servers):", (timestamp).std())
# print("min timestamp (servers):", (timestamp).min())
# print("max timestamp (servers):", (timestamp).max())
# 
# TODO: Dont compare with data, work only with thte values in servers and compare the values

# weird_timings = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=True)
# weird_timings = weird_timings.loc[(weird_timings > maxx) | (weird_timings < minn)]
# print(weird_timings)
