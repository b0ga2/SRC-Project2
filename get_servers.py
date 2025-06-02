import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import geoip2.database
import matplotlib.pyplot as plt 

datafile = 'dataset6/servers6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

up_bytes = data.groupby(['dst_ip','port', 'proto'])['up_bytes'].sum().sort_values(ascending=False)
down_bytes = data.groupby(['dst_ip','port', 'proto'])['down_bytes'].sum().sort_values(ascending=False)
ratio = pd.DataFrame(down_bytes / up_bytes, columns=['ratio'])
up_down_ratio = pd.concat([up_bytes, down_bytes, ratio], axis=1)
print(up_down_ratio.sort_values(by='ratio', ascending=False))
print("Mean Ratio: ", (down_bytes / up_bytes).mean())

