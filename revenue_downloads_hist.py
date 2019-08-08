# -*- coding: utf-8 -*-
"""
Created on Wed May 22 15:39:08 2019

@author: n_zizos
"""
import requests, sys, os
from datetime import datetime, timedelta, date
from importlib import reload
from google.cloud import storage

reload(sys)

storage_client = storage.Client.from_service_account_json('WG Mobile HQ-939f297443d2.json')
folder = 'files/'
filename = 'revenue_downloads_daily_'
gcp_bucket = 'appannie_rnd_hist'
# today = datetime.now()
# today_srt = today.strftime('%Y-%m-%d')
# yesterday = today - timedelta(days=1)
# yesterday_srt = yesterday.strftime('%Y-%m-%d')

api_key = '0e5565ec95feee9e5fea3da7cad29f5532b67685'
headers = {"Authorization": "Bearer " + api_key}
api_request = 'https://api2.appannie.com/v2/bulk/get-bulk-urls?metric_bundles=store_product_level&country_codes=all&unified_category_group=all_games&granularity=daily&start_date={}&end_date={}'


year = int(sys.argv[1])
start_date = date(year, 1, 1)
end_date = date(year, 12, 31)


def get_appanie_data(START, END):
    usage_app_history_url=api_request.format(START, END)
    r = requests.get(usage_app_history_url, headers=headers, timeout=50000)

    ##find list of urls containing the metrics name
    lst = r.json()['data']['urls']
    r_urls = [k for k in lst if 'metrics' in k]

    r.json()

    ##get list of dates for titles
    l =r_urls
    split_1=[i.split('/daily/', 1)[1] for i in l]
    list_of_dates=[i.split('/data', 1)[0] for i in split_1]

    return r_urls, list_of_dates


def check_dir(DIR):

    if not os.path.exists(DIR):
        os.makedirs(DIR)

    files = os.listdir(DIR)
    return files


def create_files(START, END):

    files = check_dir(folder)
    r_urls, list_of_dates = get_appanie_data(START, END)

    ###download zip file
    for i,j in zip(r_urls,list_of_dates):
        if any (j in file for file in files):
            print('File for {} exists'.format(j))
        else:
            print('Getting data for {}'.format(j))
            r_file = requests.get(i, headers=headers, allow_redirects=True)
            with open(folder + filename + str(j) + '.gz', 'wb') as f:
                f.write(r_file.content)
            f.close()


def load_to_gcloud():

    create_files(start_date, end_date)

    bucket = storage_client.get_bucket(gcp_bucket)
    files = check_dir(folder)
    for file in files:
        blob = bucket.blob(file)
        blob_exists = storage.Blob(bucket=bucket, name=file).exists(storage_client)
        if blob_exists:
            print('File {} exists on Cloud storage'.format(file))
        else:
            blob.upload_from_filename(folder + file)
            print('File {} uploaded to Cloud storage'.format(file))
        os.remove(folder + file)


if __name__ == '__main__':
    load_to_gcloud()
