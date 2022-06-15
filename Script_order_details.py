#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import random
from random import randint
from faker import Faker

def datagenerate(recordcount, header):
    fake =Faker('en_IN')
    with open("order_details.csv",'wt') as csvfile:
        
        writer=csv.DictWriter(csvfile,fieldnames= headers)
        writer.writeheader()
        global incremental
        incremental = 1
        for x in range(1,recordcount+1):
            
            
            #k=0
            for k in range(1,4):
                
                #m=0
                import pandas as pd
                time_stp=pd.Timestamp(fake.date_time())
                for m in range(1,4): 
                    import pandas as pd
                    #time_stp=pd.Timestamp(fake.date_time())
                    if m==1:
                        order_status_update_timestamp=time_stp
                        order_status = "Received"
                        customerid = k
                        orderid =x
                        writer.writerow(
                        {
                            "orderid":incremental,
                            "customerid":x,
                            "order_status_update_timestamp":order_status_update_timestamp,
                            "order_status":"Received",
                        }
                        )
                    elif m==2:
                        l =randint(1,3)
                        try:
                            order_status_update_timestamp=time_stp.replace(day=time_stp.day+1)
                        except:
                            try:
                                order_status_update_timestamp=time_stp.replace(month=time_stp.month+1)
                                order_status_update_timestamp=order_status_update_timestampp.replace(day=time_stp.day-5)
                            except:
                                order_status_update_timestamp=time_stp.replace(year=time_stp.year+1,month=1,day=1)
                                order_status_update_timestamp=order_status_update_timestamp.replace(day=time_stp.day-5)
                            
                        order_status = "InProgress"
                        customerid = k
                        orderid =x
                        writer.writerow(
                        {
                            "orderid":incremental,
                            "customerid":x,
                            "order_status_update_timestamp":order_status_update_timestamp,
                            "order_status":"InProgress",
                        }
                        )
                    else:
                        r = randint(4,5)
                        try:
                            order_status_update_timestamp=time_stp.replace(day=time_stp.day+2)
                        except:
                            try:
                                order_status_update_timestamp=time_stp.replace(month=time_stp.month+1)
                                
                            except:
                                order_status_update_timestamp=time_stp.replace(year=time_stp.year+1,month=1,day=1)
                                
                        order_status = "Delivered"
                        customerid = k
                        orderid =x
                        writer.writerow(
                        {
                            "orderid":incremental,
                            "customerid":x,
                            "order_status_update_timestamp":order_status_update_timestamp,
                            "order_status":"Delivered",
                        }
                        )
                    
                incremental= incremental+1
                    #m=m+1
                #k=k+1
if __name__ == '__main__':
    recordcount =6670
    headers =["orderid","customerid","order_status_update_timestamp","order_status"]
    datagenerate(recordcount, headers)
    import pandas as pd
    df = pd.read_csv("order_details.csv")
    #checking the number of empty rows in th csv file
    #print (df.isnull().sum())
    #Droping the empty rows
    modifiedDF = df.dropna()
    #Saving it to the csv file 
    modifiedDF.to_csv('order_details.csv',index=False)
    print('Done')

