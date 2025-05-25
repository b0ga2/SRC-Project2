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

service_ports = data.groupby(['port']).count()
print(service_ports)
