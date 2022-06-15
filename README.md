# Capstone-3-Project
Steps that has been executed to achieve 7 Tasks mentioned in Capstone Project:

Steps 1: Created a VPC network along with Ingress & Egress firewall for this project

Steps 2: Created a Cloud SQL instance using PostgreSQL

Steps 3: Created a OLTP schema in PostgreSQL using DDL(DDL_OLTP.sql)

Steps 4: Created 4 python scripts for generating 4 fake tables (customer_master, product_master, order_details & order_items) using python faker library.
Python Scripts are:
1. Script_customer_master.py
2. Script_product_master.py
3. Script_order_details.py
4. Script_order_items.py

Steps 5: Made a connection with DBeaver client and import 4 tables data into OLTP Schema

Steps 6: Created a Star Schema in BigQuery for 4 dimension(dim_order, dim_customer, dim_address & dim_product) & 2 Fact tables(fact_daily_orders & fact_order_details)

Steps 7: Created 6 python scripts to handle the ETL jobs, that takes care of extracting data from postgresql, tranforming the data as per
dimension/fact table requirement and loading the data into BigQuery tables.

Python scripts are:
1. dim_address_script.py
2. dim_customer_script.py
3. dim_order_script.py
4. dim_product_script.py
5. fact_daily_orders_script.py
6. fact_order_details_script.py

Steps 8: Created a DAG.py which consists of the required code to call the above python scripts using composer airflow. Trigger DAG to complete the ETL jobs

Steps 9: Steps 4,5,8 is repeated to generate another 5000 orders and populate OLTP schema. 
Note: Steps 7 always load the dalta data into BigQuery tables.

Steps 10: BigQuery Tables has been used to generate SQL queries to meet 10 Analytics Requirements.
