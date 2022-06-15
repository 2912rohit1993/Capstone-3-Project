#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import random 
from faker import Faker
from random import randint
import csv
import datetime

def datagenerate(recordcount, headers):
    fake =Faker('en_IN')

    pincodes=[]
    for i in range(26):
        pincode= randint(400066,400166)
        pincodes.append(pincode)

    
    with open("customer_master.csv",'w') as csvfile:
        writer=csv.DictWriter(csvfile,fieldnames= headers)
        writer.writeheader()
        
        for i in range(1,(recordcount+1)):
            
            state = ['Bihar','Rajasthan','Tamil Nadu','Kerala','Maharashtra','Karnataka']
            city = {'Bihar':{'Muzaffarpur','patna','Gaya','Aara','Purnia'},
                    'Rajasthan':{'Kota','Jaipur','Udaipur','Sikar'},
                    'Tamil Nadu':{'Chennai','Vellore'},
                    'Kerala':{'Munnar','Trivandrum'},
                    'Maharashtra':{'Mumbai','Pune','Nagpur','Thane','Nashik','Lonavala'},
                    'Karnataka':{'Bangalore','Mysore'}}
            
            random_state = random.choice(state)
            if random_state in city.keys():
                random_city = random.choice(list(city[random_state]))
                
            update_timestamp=fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2022, 9,6))+' '+fake.time()
            
            writer.writerow(
            {
                "customerid":i,
                "name":fake.name(),
                "address":fake.address(),
                "city":random_city,
                "state":random_state,
                "pincode":random.choice(pincodes),
                "update_timestamp":update_timestamp[:16]
            }
            )
if __name__ == '__main__':
    recordcount =1000
    headers =["customerid","name","address","city","state","pincode","update_timestamp"]
    datagenerate(recordcount, headers)
    import pandas as pd
    df = pd.read_csv("customer_master.csv")
    #checking the number of empty rows in th csv file
    #print (df.isnull().sum())
    #Droping the empty rows
    modifiedDF = df.dropna()
    #Saving it to the csv file 
    modifiedDF.to_csv('customer_master.csv',index=False)
    print('Done')

