import pandas as pd
import numpy as np

datafile = 'dataset10/data10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

up_bytes_443   = data.loc[data['port'] == 443].groupby(['dst_ip','port', 'proto'])['up_bytes'].sum().sort_values(ascending=False)
down_bytes_443 = data.loc[data['port'] == 443].groupby(['dst_ip','port', 'proto'])['down_bytes'].sum().sort_values(ascending=False)
up_bytes_53    = data.loc[data['port'] == 53].groupby(['dst_ip','port', 'proto'])['up_bytes'].sum().sort_values(ascending=False)
down_bytes_53  = data.loc[data['port'] == 53].groupby(['dst_ip','port', 'proto'])['down_bytes'].sum().sort_values(ascending=False)

ratio_53  = pd.DataFrame(down_bytes_53 / up_bytes_53, columns=['ratio'])
ratio_443 = pd.DataFrame(down_bytes_443 / up_bytes_443, columns=['ratio'])

mean_443 = (down_bytes_443 / up_bytes_443).mean()
std_443  = (down_bytes_443 / up_bytes_443).std()

mean_53 = (down_bytes_53 / up_bytes_53).mean()
std_53  = (down_bytes_53 / up_bytes_53).std()

up_down_ratio_443 = pd.concat([up_bytes_443, down_bytes_443, ratio_443], axis=1).sort_values(by='ratio', ascending=False)
up_down_ratio_443 = up_down_ratio_443.loc[up_down_ratio_443['ratio'] > mean_443 + std_443]

up_down_ratio_53 = pd.concat([up_bytes_53, down_bytes_53, ratio_53], axis=1).sort_values(by='ratio', ascending=False)
# up_down_ratio_53 = up_down_ratio_53.loc[up_down_ratio_53['ratio'] > mean_53 + std_53]

# Filter out dst_ip's that start with '192.'
# up_down_ratio_443 = up_down_ratio_443[up_down_ratio_443.index.get_level_values('dst_ip').str.startswith('192.')]

print(up_down_ratio_443.drop_duplicates())
print("Mean Ratio: ", mean_443)
print("Std: ", std_443)
print(up_down_ratio_53.drop_duplicates())
print("Mean Ratio: ", mean_53)
print("Std Ration: ", std_53)
