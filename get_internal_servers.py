import pandas as pd
import numpy as np
import ipaddress

datafile = 'dataset10/data10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

NET = ipaddress.IPv4Network('192.168.110.0/24')
bpublic = data.apply(lambda x: ipaddress.IPv4Address(x['dst_ip']) in NET, axis=1)
bpublicDF = bpublic.to_frame(name='dst_public')
newdata = pd.concat([data, bpublicDF], axis=1)

servers_443 = newdata.loc[(newdata['dst_public'] == True) & (newdata["port"] == 443)].groupby(['dst_ip']).count()
servers_53 = newdata.loc[(newdata['dst_public'] == True) & (newdata["port"] == 53)].groupby(['dst_ip']).count()
print("Servers on port 443:\n", servers_443)
print("\nServers on port 53:\n", servers_53)
