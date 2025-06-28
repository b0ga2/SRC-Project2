import pandas as pd

datafile = 'dataset10/data10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

service_ports = data.groupby(['port', 'proto']).count()
print(service_ports)
