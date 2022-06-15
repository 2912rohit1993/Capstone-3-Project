#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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

def ExtractFactDailyOrdersData():
    qry = f'''select * from customer_master;'''
    df_customer_master = pd.read_sql_query(qry,conn)
    qry1 = f'''select * from product_master;'''
    df_product_master = pd.read_sql_query(qry1,conn)
    qry2 = f'''select * from order_details;'''
    df_order_details = pd.read_sql_query(qry2,conn)
    qry3 = f'''select * from order_items;'''
    df_order_items = pd.read_sql_query(qry3,conn)
    
    x=df_order_details.groupby("orderid").head(1)[["customerid","orderid","order_status_update_timestamp"]].reset_index()
    del x["index"]  
    y=df_order_details.groupby("orderid").tail(1)["order_status_update_timestamp"].reset_index()
    del y["index"]  
    fact_daily_orders = pd.concat([x,y],axis=1)
    fact_daily_orders.columns = ["customerid","orderid","order_received_timestamp","order_delivery_timestamp"]
    l=[]
    for i in fact_daily_orders["customerid"]:
        l.append(int(df_customer_master.where(df_customer_master["customerid"]==i).dropna()["pincode"]))
        # l is pincode column
    fact_daily_orders["pincode"] = l
    fact_daily_orders
    k=[]
    for i in df_order_items["productid"]:
        k.append(int(df_product_master.where(df_product_master["productid"]==i).dropna()["rate"] * df_order_items.iloc[i,2] ))
    df_order_items["Total"]=k  
    k=[]
    k = df_order_items.groupby('orderid').sum()["Total"]
    k1 = df_order_items.groupby('orderid').sum()["quantity"]
    fact_daily_orders["order_amount"]=list(k)
    fact_daily_orders["item_count"]=list(k1)
    fact_daily_orders["order_delivery_time_seconds"] =fact_daily_orders["order_delivery_timestamp"] - fact_daily_orders["order_received_timestamp"]
    return fact_daily_orders

def TransformFactDailyOrdersData(fact_daily_orders_data):
    #fetch historical dim_customer data from Bigquery
    query_string ="""SELECT * FROM `access-3-352609.UFH_DM.fact_daily_orders` """
    fact_daily_orders = client.query(query_string).result().to_dataframe()
    
    delta_fact_daily_orders_data = fact_daily_orders_data
    #delta_product["start_date"]=np.nan
    #delta_product["end_date"]=np.nan

    #compare 
    inserts = delta_fact_daily_orders_data[~delta_fact_daily_orders_data.apply(tuple,1).isin(fact_daily_orders.apply(tuple,1))]
    return inserts

def LoadFactDailyOrdersData(inserts,tablename):
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
    required_record = ExtractFactDailyOrdersData()
    insert_record = TransformFactDailyOrdersData(required_record)
    Loaded_record = LoadFactDailyOrdersData(insert_record,'fact_daily_orders')
    print('Done')

