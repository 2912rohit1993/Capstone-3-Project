#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import random 
from faker import Faker
from random import randint
import csv
import datetime

def datagenerate(headers):
    with open("product_master.csv",'w') as csvfile:
        
        writer=csv.DictWriter(csvfile,fieldnames= headers)
        writer.writeheader()
        #50 unique product names
        product_name = ['Ata','Oranges','Apples','Bananas','Lettuce','Tomatoes','Squash',
                        'Celery','Cucumber','Mushrooms','Milk ','Cheese','Eggs','Cottage cheese',
                        'Sour cream','Yogurt','Beef','Poultry','Ham','Seafood','Lunch meat','Soda',
                        'Juice','Coffee','Tea','Water','Noodles','Rice','Canned','Dry mix','Bread',
                        'Bagels','Muffins','Cake','Potato chips','Pretzels','Ice cream','Cookies',
                        'Paper plates','Napkins','Garbage bags','Detergent','Green Tea','Potato Fry',
                        'Brinzel','Meat Curry','Chicket Curry','Wine','Red Wine','pea']

        #50 unique product codes
        product_code = [x for x in range(1,51)]
        product_codes=[]
        for i in range(0,9):
            string = 'A0'+str(product_code[i])
            product_codes.append(string)
        for i in range(9,50):
            string = 'A'+str(product_code[i])
            product_codes.append(string)


        #50 unique product price  
        prices = []
        for i in range(1,51):
            price= randint(50,100)
            prices.append(price)

        zipresult=zip(product_name,product_codes,prices)
        required_list = list(zipresult)
        
        global number
        number=1
        for i in range(len(required_list)):
            
            
            
            isactive = True
            n =3

            #n=3 sku=1kg, 5kg & 10kg
            if n==3:
                for j in range(3):
                    productid = randint(1,10000)
                    productname = required_list[i][0]
                    productcode = str(required_list[i][1])+str(j)
                    if j==0:
                        sku = '1'+'KG'
                        rates = required_list[i][2]
                        number=number
                    elif j==1:
                        sku = '5'+'KG'
                        rates = required_list[i][2]*5
                        number=number
                    else:
                        sku = '10'+'KG'
                        rates = required_list[i][2]*10
                        number=number
                    
                    writer.writerow(
                    {
                        #"productid":productid,
                        "productid":number,
                        "productcode":productname,
                        "productname":productcode,
                        "sku":sku,
                        "rate":rates,
                        "isactive":isactive
                    }
                    )
                    number=number+1
if __name__ == '__main__':
    headers =["productid","productcode","productname","sku","rate","isactive"]
    datagenerate(headers)
    import pandas as pd
    df = pd.read_csv("product_master.csv")
    #checking the number of empty rows in th csv file
    #print (df.isnull().sum())
    #Droping the empty rows
    modifiedDF = df.dropna()
    #Saving it to the csv file 
    modifiedDF.to_csv('product_master.csv',index=False)
    print('Done')

