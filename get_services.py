import pandas as pd

datafile = 'dataset6/data6.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

service_ports = data.groupby(['port', 'proto']).count()
print(service_ports)
