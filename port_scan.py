import pandas as pd

datafile = 'dataset10/servers10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

# this contains no port scans (no connections to ports other than 443 and 53) --> This is normal and expected
port_count_per_ip = data.loc[data['port'] != 443].groupby(['src_ip'])['port'].unique().apply(list).to_dict()
print(port_count_per_ip)
