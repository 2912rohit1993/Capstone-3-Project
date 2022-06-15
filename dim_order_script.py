#!/usr/bin/env python
# coding: utf-8

# In[2]:


#import libraries

import psycopg2
from psycopg2 import extras 
import pandas as pd
import os
import numpy as np
from datetime import datetime


from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes

#Connection to cloud BigQuery
credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/GoogleBigQuerykey.json')
client = bigquery.Client(credentials=credentials)

#connection to source Postgres DB
conn = psycopg2.connect(database="ufh",
                    user='postgres', password='postgre', 
                    host='35.196.236.36', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()

# Function to get delta record to insert for dim_order
# Function to get record from order_details from Postgres DB        
def ExtractDimOrderData():
    #lastrundate = getLastETLforDimAddress('dim_order')
    #qry = f'''select * from customer_master where update_timestamp > '{lastrundate}';'''
    #datetime_lastrun = datetime.now()
    qry = f'''select * from order_details;'''
    df = pd.read_sql_query(qry,conn)
    #df1 = df.loc[df.groupby(['orderid'])['order_status_update_timestamp'].idxmax()]
    return df

# ETL operation to generate order_details data
def TransformDimOrderData(dim_order_data):
    dim_order_data = dim_order_data.loc[dim_order_data.groupby(['orderid'])['order_status_update_timestamp'].idxmax()]
    dim_order_data = dim_order_data.reset_index(drop=True)
    dim_order_fields = ['orderid','order_status_update_timestamp','order_status']
    dim_order = pd.DataFrame(columns=dim_order_fields)
    #dim_order_data = ExtractDimAddressData()

    delta_order = pd.DataFrame(columns=dim_order_fields, index = range(1, len(dim_order_data)+1))

        #fetch historical dim_address data from Bigquery
    query_string ="""SELECT * FROM `access-3-352609.UFH_DM.dim_order` ORDER BY orderid DESC"""
    dim_order = client.query(query_string).result().to_dataframe()

    for i in range(1, len(dim_order_data)+1):
        delta_order['orderid'][i] = dim_order_data['orderid'][i-1]
        delta_order['order_status_update_timestamp'][i]= dim_order_data['order_status_update_timestamp'][i-1]
        delta_order['order_status'][i] = dim_order_data['order_status'][i-1]

        #compare 
    inserts = delta_order[~delta_order.apply(tuple,1).isin(dim_order.apply(tuple,1))]
    return inserts

# Loading into dim_order table in bigquery
def LoadDimOrderData(inserts,tablename):
    lastrundate = datetime.now()
    default_date = datetime.now()
    if (lastrundate== default_date):
        try:
            print('Historical data Loading ')
            #tableRef = client.dataset("UFH_DM").table('dim_order')
            tableRef = client.dataset("UFH_DM").table(tablename)
            bigqueryJob = client.load_table_from_dataframe(inserts, tableRef)
            print('Load Complete')
        except:
            print('Something goes wrong while loading')
            
# Calling main function            
if __name__ == '__main__':
    required_record = ExtractDimOrderData()
    insert_record = TransformDimOrderData(required_record)
    Loaded_record = LoadDimOrderData(insert_record,'dim_order')
    print('Done')

