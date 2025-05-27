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
std = (port443 / port53).std()

print(up_down_ratio)
print("Mean: ", mean)
print("Std: ", std)

datafile = 'dataset6/test6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

port443 = data.loc[data['port'] == 443 ].groupby(['src_ip']).count()["up_bytes"]
port53 = data.loc[data['port'] == 53].groupby(['src_ip']).count()["down_bytes"]

ratio = pd.DataFrame(port443/ port53, columns=['ratio'])
up_down_ratio = pd.concat([port443, port53, ratio], axis=1)

print(up_down_ratio)
print("Mean: ",(port443/ port53).mean())
print("Std: ",(port443/ port53).std())

# TODO: Probably adjust the value mean-std
dns_exfiltration = up_down_ratio.loc[up_down_ratio["ratio"] < (mean-(std*2))]
print(dns_exfiltration)
