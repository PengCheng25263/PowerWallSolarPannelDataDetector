from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import storage
from io import BytesIO, StringIO

import urllib3
import pandas as pd
import json5
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# to keep timestamp consistency and avoid repeated api calls, I generate dfs for three signals in one step
# one could also make this a function and run for each signal data in order to reduce the code volume
# so here's a trade-off
http = urllib3.PoolManager()
# define url path to call api: get all the sites
url1 = 'https://te-data-test.herokuapp.com/api/sites?token=1806b4f1563d83d2ce538d72a9db86a0'
req1 = http.request('GET', url1)
# transform data format
j1 = json5.loads(req1.data.decode('utf-8'))
df1 = pd.DataFrame(data = j1)
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

    if "SITE_SM_siteInstPower" in j2["signals"] and j2["signals"]["SITE_SM_siteInstPower"]:
        tmp_SITE_SM_siteInstPower.append(j2["signals"]["SITE_SM_siteInstPower"])
    else:
        tmp_SITE_SM_siteInstPower.append(None)

    if "SITE_SM_solarInstPower" in j2["signals"] and j2["signals"]["SITE_SM_solarInstPower"]:
        tmp_SITE_SM_solarInstPower.append(j2["signals"]["SITE_SM_solarInstPower"])
    else:
        tmp_SITE_SM_solarInstPower.append(None)

SITE_SM_batteryInstPower.loc[0] = tmp_SITE_SM_batteryInstPower
SITE_SM_siteInstPower.loc[0] = tmp_SITE_SM_siteInstPower
SITE_SM_solarInstPower.loc[0] = tmp_SITE_SM_solarInstPower

# read prev csv data in google storage
def read_storage_csv(bucket, path: str):
    blob = bucket.blob(path)
    byte_object = BytesIO()
    blob.download_to_file(byte_object)
    byte_object.seek(0)

    return byte_object

# define task template
def dataUpate(csv_name: str, folder_name: str, df: pd.DataFrame,
                     bucket_name = "signal-data-bucket", **kwargs):
    hook = GoogleCloudStorageHook()

    # cache prev data if exists in storage and merge with new generated df
    if hook.exists(bucket_name, object = '{}/{}.csv'.format(folder_name, csv_name)):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        f_download = read_storage_csv(bucket, path = '{}/{}.csv'.format(folder_name, csv_name))

        df_download = pd.read_csv(f_download).iloc[:, 1:]
        # append according to column names, lack value will filled with null
        # this is for situations when api returns diff sites numbers and columns might not match
        df = df_download.append(df, ignore_index=True)

    df.to_csv(csv_name)

    hook.upload(bucket_name,
                object = '{}/{}.csv'.format(folder_name, csv_name),
                filename = csv_name,
                mime_type = 'text/csv')

dag = DAG('TeslaDataProc',
          default_args = default_args,
          schedule_interval = '* * * * *', # schedule run for every minute
          catchup = True)
# three parallel tasks in this dag
with dag:
    dummy_start_up = DummyOperator(
        task_id='All_jobs_start')

    dummy_shut_down = DummyOperator(
        task_id='All_jobs_end')

    batteryInstPower_task = PythonOperator(
        task_id = 'batteryInstPowerUpdate',
        python_callable = dataUpate,
        provide_context = True,
        op_kwargs = {'csv_name': 'SITE_SM_batteryInstPower.csv', 'folder_name': 'airflow',
                     'df': SITE_SM_batteryInstPower},
    )

    siteInstPower_task = PythonOperator(
        task_id = 'siteInstPowerUpdate',
        python_callable = dataUpate,
        provide_context = True,
        op_kwargs={'csv_name': 'SITE_SM_siteInstPower.csv', 'folder_name': 'airflow',
                   'df': SITE_SM_siteInstPower},
    )

    solarInstPower_task = PythonOperator(
        task_id = 'solarInstPowerUpdate',
        python_callable = dataUpate,
        provide_context = True,
        op_kwargs = {'csv_name': 'SITE_SM_solarInstPower.csv', 'folder_name': 'airflow',
                     'df': SITE_SM_solarInstPower},
    )
    #parallel dependency and run
    for task in (batteryInstPower_task, siteInstPower_task, solarInstPower_task):
        dummy_start_up >> task >> dummy_shut_down