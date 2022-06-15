#!/usr/bin/env python
# coding: utf-8

# In[13]:


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

def ExtractDimCustomerData():
    #lastrundate = getLastETLforDimAddress('dim_order')
    #qry = f'''select * from customer_master where update_timestamp > '{lastrundate}';'''
    #datetime_lastrun = datetime.now()
    qry = f'''select * from customer_master;'''
    df = pd.read_sql_query(qry,conn)
    #df1 = df.loc[df.groupby(['orderid'])['order_status_update_timestamp'].idxmax()]
    return df[['customerid','name','update_timestamp']]

def TransformDimCustomerData(dim_customer_data):
    
    #dim_customer_fields = ['customerid','name','address_id','start_date','end_date']
    #dim_customer = pd.DataFrame(columns=dim_customer_fields)
    #dim_order_data = ExtractDimAddressData()

    #delta_order = pd.DataFrame(columns=dim_order_fields, index = range(1, len(dim_customer_data)+1))

    #fetch historical dim_customer data from Bigquery
    query_string ="""SELECT * FROM `access-3-352609.UFH_DM.dim_customer` """
    dim_customer = client.query(query_string).result().to_dataframe()

    dim_customer_data["address_id"] = list(range(1,1001))
    delta_order =dim_customer_data[["customerid","name","address_id"]]
    delta_order["start_date"]= list(dim_customer_data['update_timestamp'].dt.date)
    delta_order["end_date"] = '9999-12-31'
    

        #compare 
    inserts = delta_order[~delta_order.apply(tuple,1).isin(dim_customer.apply(tuple,1))]
    return inserts
def LoadDimCustomerData(inserts,tablename):
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
    required_record = ExtractDimCustomerData()
    insert_record = TransformDimCustomerData(required_record)
    Loaded_record = LoadDimCustomerData(insert_record,'dim_order')
    print('Done')

