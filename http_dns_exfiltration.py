import pandas as pd

datafile = 'dataset6/data6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

# Number of connections
port443 = data.loc[data['port'] == 443 ].groupby(['src_ip']).count()["up_bytes"]
port53 = data.loc[data['port'] == 53].groupby(['src_ip']).count()["down_bytes"]

# Ratio of connections
ratio = pd.DataFrame(port443 / port53, columns=['ratio'])
up_down_ratio = pd.concat([port443, port53, ratio], axis=1)

mean = (port443 / port53).mean()
std  = (port443 / port53).std()
minn = (port443 / port53).min()
maxx = (port443 / port53).max()

print(up_down_ratio)
print("Mean (data): ", mean)
print("Std (data): ", std)
print("min (data): ", minn)
print("max (data): ", maxx)

datafile = 'dataset6/test6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

port443 = data.loc[data['port'] == 443 ].groupby(['src_ip']).count()["up_bytes"]
port53 = data.loc[data['port'] == 53].groupby(['src_ip']).count()["down_bytes"]

ratio = pd.DataFrame(port443/ port53, columns=['ratio'])
up_down_ratio = pd.concat([port443, port53, ratio], axis=1)

print(up_down_ratio)
print("Mean (servers): ",(port443 / port53).mean())
print("Std (servers): ",(port443 / port53).std())
print("min (servers): ",(port443 / port53).min())
print("max (servers): ",(port443 / port53).max())

dns_exfiltration = up_down_ratio.loc[up_down_ratio["ratio"] < minn]
print(dns_exfiltration)
