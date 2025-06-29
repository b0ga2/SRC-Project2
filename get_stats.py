import pandas as pd
import numpy as np
import geoip2.database

def get_cc(ip):
    try: return geoCC.country(ip).country.iso_code
    except: return None

def get_asn(ip):
    try: return geoASN.asn(ip).autonomous_system_organization
    except: return None

datafile = 'dataset10/data10.parquet'

### Read parquet data files
data = pd.read_parquet(datafile)

### IP geolocalization
geoCC = geoip2.database.Reader('GeoLite2/GeoLite2-Country.mmdb')
geoASN = geoip2.database.Reader('GeoLite2/GeoLite2-ASN.mmdb')

geo_loc_cc = data.apply(lambda x: get_cc(x['dst_ip']), axis=1)
geo_loc_asn = data.apply(lambda x: get_asn(x['dst_ip']), axis=1)
geo_loc_cc_DF = geo_loc_cc.to_frame(name='cc')
geo_loc_asn_DF = geo_loc_asn.to_frame(name='asn')
newdata = pd.concat([data, geo_loc_cc_DF, geo_loc_asn_DF], axis=1)

unique_ccs = set(newdata['cc'])
if None in unique_ccs:
    unique_ccs.remove(None)

unique_asn = set(newdata['asn'])
if None in unique_asn:
    unique_asn.remove(None)

up_bytes_443 = newdata.loc[data['port'] == 443].groupby(['src_ip', 'port', 'proto'])['up_bytes'].sum().sort_values(ascending=False)
up_bytes_53 = newdata.loc[data['port'] == 53].groupby(['src_ip', 'port', 'proto'])['up_bytes'].sum().sort_values(ascending=False)
down_bytes_443 = newdata.loc[data['port'] == 443].groupby(['src_ip', 'port','proto'])['down_bytes'].sum().sort_values(ascending=False)
down_bytes_53 = newdata.loc[data['port'] == 53].groupby(['src_ip', 'port','proto'])['down_bytes'].sum().sort_values(ascending=False)


ratio_443 = pd.DataFrame(down_bytes_443 / up_bytes_443, columns=['ratio'])
ratio_53 = pd.DataFrame(down_bytes_53 / up_bytes_53, columns=['ratio'])

up_down_ratio_443 = pd.concat([up_bytes_443, down_bytes_443, ratio_443], axis=1)
up_down_ratio_53 = pd.concat([up_bytes_53, down_bytes_53, ratio_53], axis=1)

print(up_down_ratio_443.sort_values(by='ratio', ascending=False))
print("mean ratio port 443:", (down_bytes_443 / up_bytes_443).mean())
print("std ratio port 443:", (down_bytes_443 / up_bytes_443).std())
print("min ratio port 443:", (down_bytes_443 / up_bytes_443).min())
print("max ratio port 443:", (down_bytes_443 / up_bytes_443).max())

print(up_down_ratio_53.sort_values(by='ratio', ascending=False))
print("mean ratio port 53:", (down_bytes_53 / up_bytes_53).mean())
print("std ratio port  53:", (down_bytes_53 / up_bytes_53).std())
print("min ratio port  53:", (down_bytes_53 / up_bytes_53).min())
print("max ratio port  53:", (down_bytes_53 / up_bytes_53).max())

print("CCs: ", unique_ccs)
print("ASNs: ", unique_asn)
