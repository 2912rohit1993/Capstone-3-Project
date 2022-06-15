#!/usr/bin/env python
# coding: utf-8

# In[24]:


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

def ExtractFactOrderDetailsData():
    qry = f'''select * from order_details;'''
    df_order_details = pd.read_sql_query(qry,conn)
    qry1 = f'''select * from order_items;'''
    df_order_items = pd.read_sql_query(qry1,conn)
    df_order_details_refined = df_order_details.loc[df_order_details.groupby(['orderid'])['order_status_update_timestamp'].idxmax()]
    f_order_details = pd.merge(df_order_details_refined,df_order_items,how='inner')[["orderid","order_status_update_timestamp","productid","quantity"]]
    f_order_details.columns = ["orderid","order_delivery_timestamp","productid","quantity"]
    return f_order_details

def TransformFactOrderDetailsData(fact_order_details_data):

    #fetch historical dim_customer data from Bigquery
    query_string ="""SELECT * FROM `access-3-352609.UFH_DM.fact_order_details` """
    fact_order_details = client.query(query_string).result().to_dataframe()
    
    delta_fact_order_details = fact_order_details_data
    #delta_product["start_date"]=np.nan
    #delta_product["end_date"]=np.nan

    #compare 
    inserts = delta_fact_order_details[~delta_fact_order_details.apply(tuple,1).isin(fact_order_details.apply(tuple,1))]
    return inserts

def LoadFactOrderDetailsData(inserts,tablename):
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
    required_record = ExtractFactOrderDetailsData()
    insert_record = TransformFactOrderDetailsData(required_record)
    Loaded_record = LoadFactOrderDetailsData(insert_record,'fact_order_details')
    print('Done')

