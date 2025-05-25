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

geo_loc_cc = data.apply(lambda x: get_cc(x['dst_ip']), axis=1)
geo_loc_asn = data.apply(lambda x: get_asn(x['dst_ip']), axis=1)
geo_loc_cc_DF = geo_loc_cc.to_frame(name='cc')
geo_loc_asn_DF = geo_loc_asn.to_frame(name='asn')
newdata = pd.concat([data, geo_loc_cc_DF, geo_loc_asn_DF], axis=1)

unique_ccs = set(newdata['cc'])
if None in unique_ccs:
    unique_ccs.remove(None)

#print(newdata)

cc = newdata.loc[newdata['cc'] != None].groupby(['cc'])['dst_ip'].unique()
print(cc)

## Country codes to exclude
ips_per_cc_dict = newdata.loc[newdata['cc'].notna()].groupby('cc')['dst_ip'].unique().apply(list).to_dict()
print("RU: ", ips_per_cc_dict["RU"][:10])
print("CN: ", ips_per_cc_dict["CN"][:10])
print("IL: ", ips_per_cc_dict['IL'][:10])
print("BH: ", ips_per_cc_dict['BH'][:10])
print("IR: ", ips_per_cc_dict['IR'][:10])
print("KR: ", ips_per_cc_dict['KR'][:10])
