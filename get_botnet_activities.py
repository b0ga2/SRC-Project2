import pandas as pd

import pandas as pd

datafile = 'dataset10/data10.parquet'

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

datafile = 'dataset10/test10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

port443 = data.loc[data['port'] == 443].groupby(['src_ip']).count()["up_bytes"]
port53 = data.loc[data['port'] == 53].groupby(['src_ip']).count()["down_bytes"]

ratio = pd.DataFrame(port443/ port53, columns=['ratio'])
up_down_ratio = pd.concat([port443, port53, ratio], axis=1)

dns_exfiltration = up_down_ratio.loc[up_down_ratio["ratio"] < minn]

baseline = pd.read_parquet('dataset10/data10.parquet')
current  = pd.read_parquet('dataset10/test10.parquet')

key_cols = ['src_ip', 'dst_ip', 'port']    

known_good = baseline[key_cols].drop_duplicates()

labelled = (current.merge(known_good.assign(_seen=True),on=key_cols,how='left',indicator=True))

new_connections = labelled[labelled['_merge'] == 'left_only'].drop(columns=['_merge', '_seen', 'timestamp', 'up_bytes', 'down_bytes', 'proto', 'port']).drop_duplicates()

a = new_connections.groupby(['src_ip']).count()
mean = a["dst_ip"].mean()
std = a["dst_ip"].std()

print(a.sort_values(by='dst_ip', ascending=False))
print("mean:", mean)
print("std:", std)

b = a.loc[a["dst_ip"] > mean + std]
print("\n\nBotnet Addresses:\n",b.sort_values(by="dst_ip", ascending=False))
