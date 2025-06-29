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

# print(up_down_ratio)
# print("Mean (data): ", mean)
# print("Std (data): ", std)
# print("min (data): ", minn)
# print("max (data): ", maxx)

datafile = 'dataset10/test10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

port443 = data.loc[data['port'] == 443].groupby(['src_ip']).count()["up_bytes"]
port53 = data.loc[data['port'] == 53].groupby(['src_ip']).count()["down_bytes"]

ratio = pd.DataFrame(port443/ port53, columns=['ratio'])
up_down_ratio = pd.concat([port443, port53, ratio], axis=1)

# print(up_down_ratio)
# print("Mean (servers): ",(port443 / port53).mean())
# print("Std (servers): ",(port443 / port53).std())
# print("min (servers): ",(port443 / port53).min())
# print("max (servers): ",(port443 / port53).max())

dns_exfiltration = up_down_ratio.loc[up_down_ratio["ratio"] < minn]

# --- 1. Load datasets -------------------------------------------------
baseline = pd.read_parquet('dataset10/data10.parquet')   # “normal” traffic
current  = pd.read_parquet('dataset10/test10.parquet')   # traffic to inspect

# --- 2. Decide what a “connection” means ------------------------------
# Typical choice: src_ip + dst_ip + destination port
key_cols = ['src_ip', 'dst_ip', 'port']      # adjust if you need more/less detail

# --- 3. Build the whitelist of known‑good connection keys -------------
known_good = baseline[key_cols].drop_duplicates()

# --- 4. Mark every row in *current* traffic as NEW / KNOWN -----------
# (left merge + indicator gives us a little “_merge” flag)
labelled = (
    current
    .merge(
        known_good.assign(_seen=True),       # gives us a column to merge on
        on=key_cols,
        how='left',
        indicator=True                       # adds _merge col
    )
)

# --- 5. Pull out the rows that are LEFT‑ONLY (“new” traffic) ----------
new_connections = labelled[labelled['_merge'] == 'left_only'].drop(columns=['_merge', '_seen', 'timestamp', 'up_bytes', 'down_bytes', 'proto', 'port']).drop_duplicates()

a = new_connections.groupby(['src_ip']).count()
mean = a["dst_ip"].mean()
std = a["dst_ip"].std()

print("==== Totally NEW connection tuples ====")
print(a.sort_values(by='dst_ip', ascending=False))
print("mean:", mean)
print("std:", std)

b = a.loc[a["dst_ip"] > mean + std]
print(b.sort_values(by="dst_ip", ascending=False))
