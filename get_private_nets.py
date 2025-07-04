import pandas as pd
import ipaddress

datafile = 'dataset10/data10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

private_nets = data.groupby(['src_ip']).count()
ip_list = private_nets.index.to_list()
ip_objects = [ipaddress.IPv4Address(ip) for ip in ip_list]

# Find the smallest and largest IP
first_ip = min(ip_objects)
last_ip = max(ip_objects)

print(first_ip)
print(last_ip)
