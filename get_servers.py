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
print((down_bytes / up_bytes).mean())

### Another approach
NET = ipaddress.IPv4Network('200.0.0.0/24')
bpublic = data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) in NET, axis=1)
bpublicDF = bpublic.to_frame(name='dst_public')
newdata = pd.concat([data, bpublicDF], axis=1)
servers_up = newdata.loc[newdata['dst_public'] == True].groupby(['dst_ip','port', 'proto'])['up_bytes'].sum()
servers_down = newdata.loc[newdata['dst_public'] == True].groupby(['dst_ip','port', 'proto'])['down_bytes'].sum()
servers_up_down = pd.concat([servers_up, servers_down], axis=1)
print(servers_up_down)
