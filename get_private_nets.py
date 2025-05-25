import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import geoip2.database
import matplotlib.pyplot as plt 

datafile = 'dataset6/data6.parquet'

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
