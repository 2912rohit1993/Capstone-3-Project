#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import random 
from faker import Faker
from random import randint
import csv

def datagenerate(recordcount, headers):
    fake =Faker('en_IN')
    with open('order_items.csv','wt') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        
        for i in range(1,recordcount+1):
            for x in range(3):
                
                writer.writerow(
                    {
                        "orderid":i,
                        "productid":fake.random_int(1,150),
                        "quantity":fake.random_int(2,7)
                    }
                                )
if __name__ == '__main__':
    recordcount =20000
    headers =["orderid","productid","quantity"]
    datagenerate(recordcount, headers)
    import pandas as pd
    df = pd.read_csv("order_items.csv")
    #checking the number of empty rows in th csv file
    #print (df.isnull().sum())
    #Droping the empty rows
    modifiedDF = df.dropna()
    #Saving it to the csv file 
    modifiedDF.to_csv('order_items.csv',index=False)
    print('Done')

