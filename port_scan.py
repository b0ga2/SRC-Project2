import pandas as pd

datafile = 'dataset6/servers6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

# TODO: this contains no port scans (no connections to ports other than 443 and 53)
port_count_per_ip = data.loc[data['port'] != 443].groupby(['src_ip'])['port'].unique().apply(list).to_dict()
print(port_count_per_ip)
