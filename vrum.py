import pandas as pd
import numpy as np
import ipaddress
import dns.resolver
import dns.reversename
import geoip2.database
import matplotlib.pyplot as plt 

datafile = 'dataset6/test6.parquet'

def get_cc(ip):
    try: return geoCC.country(ip).country.iso_code
    except: return None

def get_asn(ip):
    try: return geoASN.asn(ip).autonomous_system_organization
    except: return None

### Read parquet data files
data = pd.read_parquet(datafile)

### IP geolocalization
geoCC = geoip2.database.Reader('GeoLite2/GeoLite2-Country.mmdb')
geoASN = geoip2.database.Reader('GeoLite2/GeoLite2-ASN.mmdb')

geo_loc_cc = data.apply(lambda x: get_cc(x['src_ip']), axis=1)
geo_loc_asn = data.apply(lambda x: get_asn(x['src_ip']), axis=1)
geo_loc_cc_DF = geo_loc_cc.to_frame(name='cc')
geo_loc_asn_DF = geo_loc_asn.to_frame(name='asn')
newdata = pd.concat([data, geo_loc_cc_DF, geo_loc_asn_DF], axis=1)

unique_ccs = set(newdata['cc'])
if None in unique_ccs:
    unique_ccs.remove(None)

up_bytes = data.loc[(data['src_ip'] == '200.0.0.11') | (data['src_ip'] == "200.0.0.12")].groupby(['src_ip'])['up_bytes'].sum().sort_values(ascending=False)
down_bytes = data.loc[(data['src_ip'] == '200.0.0.11') | (data['src_ip'] == "200.0.0.12")].groupby(['src_ip'])['down_bytes'].sum().sort_values(ascending=False)
data['diff_timestamp'] = data.groupby(['src_ip'])['timestamp'].diff().fillna(0)
timestamp = data.groupby(['src_ip'])['diff_timestamp'].mean().sort_values(ascending=False)
ratio = pd.DataFrame(down_bytes / up_bytes, columns=['ratio'])
up_down_ratio = pd.concat([up_bytes, down_bytes, ratio,timestamp], axis=1)
print(up_down_ratio.sort_values(by='ratio', ascending=False))
print("mean:", (down_bytes / up_bytes).mean())
print("std:", (down_bytes / up_bytes).std())
print("mean timestamp:", (timestamp).mean())
print("std timestamp:", (timestamp).std())
print("CCs: ", unique_ccs)
