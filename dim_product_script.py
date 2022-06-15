#!/usr/bin/env python
# coding: utf-8

# In[18]:


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

def ExtractDimProductData():
    qry = f'''select * from product_master;'''
    df = pd.read_sql_query(qry,conn)
    return df

def TransformDimProductData(dim_product_data):

    #fetch historical dim_customer data from Bigquery
    query_string ="""SELECT * FROM `access-3-352609.UFH_DM.dim_product` """
    dim_product = client.query(query_string).result().to_dataframe()
    
    delta_product = dim_product_data
    #delta_product["start_date"]=np.nan
    #delta_product["end_date"]=np.nan

    #compare 
    inserts = delta_product[~delta_product.apply(tuple,1).isin(dim_product.apply(tuple,1))]
    return inserts

def LoadDimProductData(inserts,tablename):
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
    required_record = ExtractDimProductData()
    insert_record = TransformDimProductData(required_record)
    Loaded_record = LoadDimProductData(insert_record,'dim_product')
    print('Done')
        
