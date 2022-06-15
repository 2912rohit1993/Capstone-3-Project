#!/usr/bin/env python
# coding: utf-8

# In[8]:


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

#default date for initial load
format_data = "%d/%m/%Y %H:%M:%S.%f"
time_data = "01/01/1900 00:00:0.000"
default_date = datetime.strptime(time_data,format_data)

# Function to insert log details in Bigquery

def insertetllog(tablename, etlrowcount, status):
    try:
        #Declare etlid value for log insert
        query_string ="""SELECT max(etlid) as max_etlid FROM `access-3-352609.UFH_DM.etl_logs` """
        max_etlid = client.query(query=query_string).result().to_dataframe()['max_etlid'][0]
        etlid = max_etlid +1
        

        # set record attributes
        record = {"etlid":etlid,"tablename":tablename,"etlrowcount": etlrowcount,
                  "etldatetime":datetime.now(),"status":status}
        
        table_ref = client.dataset("UFH_DM").table("etl_logs")
        df_record = pd.DataFrame(record,index=[0])
        client.load_table_from_dataframe(df_record,table_ref)

    except Exception as e:
        print("Unable to insert record into etl logs" + print(str(e)))# Function to insert log details in Bigquery

# function to get last eTL for dim_address table
#default date for initial load
def getLastETLforDimAddress(tblname):
    try:
        query = f"""select max(etldatetime) as max_etldatetime from `access-3-352609.UFH_DM.etl_logs` where tablename = '{tblname}'"""
        etlrundate = client.query(query).result().to_dataframe()['max_etldatetime'][0]
        return etlrundate
    except Exception  as e:
        return default_date

# Function to get delta record to insert for dim_address
# Function to get record from customer_master from Postgres DB        
def ExtractDimAddressData():
    lastrundate = getLastETLforDimAddress('dim_address')
    #qry = f'''select * from customer_master where update_timestamp > '{lastrundate}';'''
    datetime_lastrun = datetime.now()
    qry = f'''select * from customer_master where update_timestamp < '{datetime_lastrun}';'''
    df = pd.read_sql_query(qry,conn)
    return df

# ETL operation to generate dim_address data
def TransformDimAddressData(dim_aaddress_data):
    dim_address_fields = ['address_id','address','city','state','pincode']
    dim_address = pd.DataFrame(columns=dim_address_fields)
    #dim_aaddress_data = ExtractDimAddressData()

    delta_address = pd.DataFrame(columns=dim_address_fields, index = range(1, len(dim_aaddress_data)+1))

        #fetch historical dim_address data from Bigquery
    query_string ="""SELECT * FROM `access-3-352609.UFH_DM.dim_address` ORDER BY address_id DESC"""
    dim_addres = client.query(query_string).result().to_dataframe()

    for i in range(1, len(dim_aaddress_data)):
        delta_address['address_id'][i] = i
        delta_address['address'][i]= dim_aaddress_data['address'][i-1]
        delta_address['city'][i] = dim_aaddress_data['city'][i-1]
        delta_address['state'][i] = dim_aaddress_data['state'][i-1]
        delta_address['state'][i] = dim_aaddress_data['state'][i-1] 
        delta_address['pincode'][i] = dim_aaddress_data['pincode'][i-1]

        #compare 
    inserts = delta_address[~delta_address.apply(tuple,1).isin(dim_addres.apply(tuple,1))]
    return inserts

def LoadDimAddressData(inserts,tablename):
    lastrundate = datetime.now()
    default_date = datetime.now()
    if (lastrundate== default_date):
        try:
            print('Historical data Loading ')
            #tableRef = client.dataset("UFH_DM").table('dim_address')
            tableRef = client.dataset("UFH_DM").table(tablename)
            bigqueryJob = client.load_table_from_dataframe(inserts, tableRef)
            print('Load Complete')
        except:
            print('Something goes wrong while loading')
            
# Calling main function            
if __name__ == '__main__':
    required_record = ExtractDimAddressData()
    insert_record = TransformDimAddressData(required_record)
    Loaded_record = LoadDimAddressData(insert_record,'dim_address')
    print('Done')
