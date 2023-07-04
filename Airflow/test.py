import urllib3
import pandas as pd
import json5
from datetime import datetime

# to keep timestamp consistency and avoid repeated api calls, I generate dfs for three signals in one step
# could also make this a function and run for each signal data in order to reduce the code volume
# so here's a trade-off
http = urllib3.PoolManager()
# define url path to call api: get all the sites
url1 = 'https://te-data-test.herokuapp.com/api/sites?token=1806b4f1563d83d2ce538d72a9db86a0'
req1 = http.request('GET', url1)
# transform data format
j1 = json5.loads(req1.data.decode('utf-8'))
df1 = pd.DataFrame(data = j1)
# print(df1)
# create header for the tables of the three parameters
sites = list(df1["sites"])
header = ["timestamp"] + sites

SITE_SM_batteryInstPower = pd.DataFrame(columns = header)
SITE_SM_siteInstPower = pd.DataFrame(columns = header)
SITE_SM_solarInstPower = pd.DataFrame(columns = header)

# define tmp lists and call api to insert data, keep the same timestamp here for the three list
tmp_SITE_SM_batteryInstPower = [df1['timestamp'][1]]
tmp_SITE_SM_siteInstPower = [df1['timestamp'][1]]
tmp_SITE_SM_solarInstPower = [df1['timestamp'][1]]

for site in sites:
    url2 = 'https://te-data-test.herokuapp.com/api/signals?token=1806b4f1563d83d2ce538d72a9db86a0&site=' + str(site)
    req2 = http.request('GET', url2)
    j2 = json5.loads(req2.data.decode('utf-8'))
    df2 = pd.DataFrame(data = j2)
    if "SITE_SM_batteryInstPower" in j2["signals"] and j2["signals"]["SITE_SM_batteryInstPower"]:
        tmp_SITE_SM_batteryInstPower.append(j2["signals"]["SITE_SM_batteryInstPower"])
    else:
        tmp_SITE_SM_batteryInstPower.append(None)

    if "SITE_SM_siteInstPower" in j2["signals"]:
        tmp_SITE_SM_siteInstPower.append(j2["signals"]["SITE_SM_siteInstPower"])
    else:
        tmp_SITE_SM_siteInstPower.append(None)

    if "SITE_SM_solarInstPower" in j2["signals"]:
        tmp_SITE_SM_solarInstPower.append(j2["signals"]["SITE_SM_solarInstPower"])
    else:
        tmp_SITE_SM_solarInstPower.append(None)

SITE_SM_batteryInstPower.loc[0] = tmp_SITE_SM_batteryInstPower
SITE_SM_siteInstPower.loc[0] = tmp_SITE_SM_siteInstPower
SITE_SM_solarInstPower.loc[0] = tmp_SITE_SM_solarInstPower

file = pd.read_csv('airflow_SITE_SM_batteryInstPower.csv').iloc[:, 1:]

# if len(file)
print(SITE_SM_batteryInstPower)
print(file)
SITE_SM_batteryInstPower = file.append(SITE_SM_batteryInstPower, ignore_index=True)
SITE_SM_batteryInstPower.to_csv("test.csv")
