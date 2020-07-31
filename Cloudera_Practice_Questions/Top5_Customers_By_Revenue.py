'''

Tables should be in hive database - <YOUR_USER_ID>_retail_db_txt
orders
order_items
customers
Time to create database and tables need not be counted. Make sure to go back to Spark SQL module and create tables and load data
Get details of top 5 customers by revenue for each month
We need to get all the details of the customer along with month and revenue per month
Data need to be sorted by month in ascending order and revenue per month in descending order
Create table top5_customers_per_month in <YOUR_USER_ID>_retail_db_txt
Insert the output into the newly created table



use gmadhu89_retail_db_txt;
create table orders
(order_id int,
order_date string,
order_customer_id int,
order_status string)
row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/orders/part-00000' into table orders;


create table order_items
(
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
)
row format delimited fields terminated by ','
stored as textfile;

#load data inpath '/public/retail_db/orders/part-00000' into table order_items;
load data local inpath '/data/retail_db/order_items/part-00000' into table order_items;


create table customers
(
customerid int,
customer_fname string,
customer_lname string,
customer_email string,
customer_password string,
customer_street string,
customer_city string,
customer_state string,
customer_zipcode string
)
row format delimited fields terminated by ','
stored as textfile;

load data local inpath '/data/retail_db/customers/part-00000' into table customers;

'''

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Top 5 Customers By Revenue").getOrCreate()

sql_cmd = ("select c.*,cast(concat(substr(o.order_date,1,4),substr(o.order_date,6,2)) as int) as Month,round(sum(oi.order_item_subtotal),2) as Revenue \
 from gmadhu89_retail_db_txt.orders o, gmadhu89_retail_db_txt.customers c, gmadhu89_retail_db_txt.order_items oi \
where o.order_customer_id = c.customerid \
and o.order_id = oi.order_item_order_id \
group by customerid,customer_fname,customer_lname,customer_email,customer_password,customer_street, \
customer_city,customer_state,customer_zipcode,cast(concat(substr(o.order_date,1,4),substr(o.order_date,6,2)) as int)")

Customer_Revenue=spark.sql(sql_cmd)

Customer_Revenue.createOrReplaceTempView("Customer_Revenue")

sql_cmd="select customerid, customer_fname, customer_lname, customer_email, customer_password, customer_street, customer_city, customer_state, customer_zipcode, \
Month,Revenue from ( \
select customerid, customer_fname, customer_lname, customer_email, customer_password, customer_street, customer_city, customer_state, customer_zipcode, \
Month,Revenue,rank() over (partition By Month order by Revenue desc) as Rank from Customer_Revenue) \
where Rank <= 5 order by Month,Revenue desc"

Top5_Customer=spark.sql(sql_cmd)

spark.sql("drop table if exists gmadhu89_retail_db_txt.top5_customers_per_month")
Top5_Customer.write.saveAsTable("gmadhu89_retail_db_txt.top5_customers_per_month")

from pyspark.sql import Window
from pyspark.sql.functions import concat,substring
from pyspark.sql.functions import rank,col

spec = Window.partitionBy("Month").orderBy(customerRevenue.Revenue.desc())
Top5Customers = customerRevenue.withColumn("Rnk",rank().over(spec)) \
.orderBy('Month',col('Rnk').desc()).where(col('Rnk') <= 5)
Top5Customers.drop('Rnk')
