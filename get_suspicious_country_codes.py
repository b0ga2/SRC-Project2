import pandas as pd
import geoip2.database

datafile = 'dataset10/data10.parquet'

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

cc_safe = newdata.loc[newdata['cc'] != None].groupby(['cc'])['down_bytes'].count()

datafile = 'dataset10/test10.parquet'

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

cc = newdata.loc[newdata['cc'] != None].groupby(['cc'])['up_bytes'].count()
ratio = pd.DataFrame(cc / cc_safe, columns=['ratio'])

# concat and sort by ratio
finaldata = pd.concat([cc_safe, cc, ratio], axis=1).sort_values(by='ratio', ascending=False)
sus_countries = finaldata.loc[(finaldata["ratio"].isna() & (finaldata["up_bytes"] > 50)) | (finaldata["ratio"] > 2)]
print(sus_countries)

# sus_ccs = sus_countries.index.tolist()
# # print(sus_ccs)
# ips_by_cc = newdata[newdata['cc'].isin(sus_ccs)].groupby('cc')['dst_ip'].unique()
# # print(ips_by_cc)
# for cc, ip_list in ips_by_cc.items():
#     print(f"{cc}: {len(ip_list)} IPs")
#     for ip in ip_list:
#         print(f"    {ip}")
#      print()

sus_ccs = sus_countries.index.tolist()
rows_sus = newdata[newdata['cc'].isin(sus_ccs)]
ips_per_country = rows_sus.groupby('cc')['dst_ip'].agg(lambda s: sorted(s.unique()))

for cc, ip_list in ips_per_country.items():
    print(f"{cc} - {len(ip_list)} unique IPs")
    for ip in ip_list:
        print(f"   {ip}")
    print()
