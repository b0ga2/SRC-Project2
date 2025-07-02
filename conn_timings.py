import pandas as pd

datafile = 'dataset10/servers10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp']

mean = timestamp.mean().to_frame(name='mean')
std  = timestamp.std().to_frame(name='std')
minn = timestamp.min().to_frame(name='min')
maxx = timestamp.max().to_frame(name='max')

table = pd.concat([mean, std, minn, maxx], axis=1).sort_values(by="std", ascending=False)
print(table)
print("STD - ",timestamp.mean().std())
print("Max - ",timestamp.mean().max())

print("\nAnomalous Use:\n",std.loc[std["std"] < timestamp.mean().std()])
print("\nAnother Approach:\n",maxx.loc[maxx["max"] < timestamp.mean().max()])
