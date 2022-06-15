--Creating Table for customer_table 

CREATE TABLE customer_master(
customerid bigint,
name varchar(100) NOT NULL,
address	varchar(254),
city varchar(60),
state varchar(60),
pincode bigint,
update_timestamp timestamp NOT NULL,
CONSTRAINT cust_master_PK PRIMARY KEY (customerid)
);


--Creating Table for product_master
CREATE TABLE product_master(
productid integer,
productcode varchar(20) NOT NULL,
productname	varchar(100),
sku	varchar(20),
rate numeric(20),
isactive boolean,
CONSTRAINT prod_master_PK PRIMARY KEY (productid)
);


--Creating Table for order_details
CREATE TABLE order_details(			
orderid	bigint,
customerid bigint NOT NULL,
order_status_update_timestamp timestamp,
order_status varchar(50),
CONSTRAINT ord_detail_PK PRIMARY KEY (orderid),
CONSTRAINT ord_detail_FK FOREIGN KEY (customerid) REFERENCES customer_master(customerid)
);
alter table order_details drop constraint ord_detail_PK;
alter table order_details ADD CONSTRAINT ord_detail_PK PRIMARY KEY (orderid,customerid,order_status);


--Creating Table for order_items
CREATE TABLE order_items(
orderid	bigint,
productid integer,
quantity integer NOT NULL,
CONSTRAINT ord_item_PK PRIMARY KEY (orderid,productid,quantity),
CONSTRAINT ord_item_FK_prod FOREIGN KEY (productid) REFERENCES product_master(productid)
);

alter table order_items drop constraint ord_item_PK;

