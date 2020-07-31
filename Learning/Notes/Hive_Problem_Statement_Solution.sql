

/*
	- Use retail_db dataset
	- Problem Statement
	
	1) Create oRDER and ORDER_ITEMS tables in Hive database YOUR_USER_ID_retail_db_txt in text file format and load into Hive tables.
	2) Create oRDER and ORDER_ITEMS tables in Hive database YOUR_USER_ID_retail_db_txt in orc file format and load into Hive tables.
	3) Get daily revenue by product considering completed and closed orders using Dataframes
	4) Products have to be read from local file system. Dataframe needs to be created.
	5) Join ORDERS, ORDER_ITEMS. 
	6) Filter on order_status.
	7) Data needs to be sorted by ascending order by date and descending order by revenue computed for each product for each day
		a. Sort data by order_date in ascending order and then daily revenue per product in descending order.
	8) After done, save data back to HDFS using Hive YOUR_USER_ID_retail_db_txt
	9) Data for products is available locally under /data/retail_db/products. Create dataframe and join with two other tables.
	
Store solution under /home/gmadhu89/daily_revenue_python_sql.txt
*/


## Operations in HIve
##Creating table as Text Files in HIve
use gmadhu89_retail_db;
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



## Creating tables in ORC format and loading from other table stored in Text File format.
use gmadhu89_retail_db_orc;
create table orders
(order_id int,
order_date string,
order_customer_id int,
order_status string)
stored as orc;
insert into orders select * from gmadhu89_retail_db.orders;


create table order_items
(
order_item_id int,
order_item_order_id int,
order_item_product_id int,
order_item_quantity int,
order_item_subtotal float,
order_item_product_price float
)
stored as orc;
insert into order_items select * from gmadhu89_retail_db.order_items;

##Below Queries can be written in Spark
pyspark --master yarn --num-executors 2 --executor-memory 512M --packages com.databricks:spark-avro_2.10:2.0.1

#from pyspark import SparkContext,SparkConf
#from pyspark.sql import HiveContext
from pyspark import Row

#conf=SparkConf().setAppName('Problem Statement 2 solution')
#sc=SparkContext(conf=conf)
hivecontext=HiveContext(sc)

/* Two ways of bringing content into a Dataframe
1) Create a DF directly using SparkContext/HiveContext
2) Create an RDD with file from HDFS and use Row objects to create a Dataframe
Register DF as temp table
3) To get data from Local file system, use python API to read file as Python collection, convert to RDD using sc.parallelize.
*/


#Getting Products Data from local file system
products=open("/data/retail_db/products/part-00000").read().splitlines()
productsRDD=sc.parallelize(products)
productsDF=productsRDD.map(lambda x:Row(product_id=int(x.split(",")[0]),product_name=x.split(",")[2])).toDF()
hivecontext.sql('use gmadhu89_retail_db_orc')
productsDF.registerTempTable("products")

#hivecontext.sql('select * from products').show()

ordersDF=hivecontext.sql('select * from gmadhu89_retail_db_orc.orders')
ordersDF.registerTempTable("orders")

order_itemsDF=hivecontext.sql('select * from gmadhu89_retail_db_orc.order_items')
order_itemsDF.registerTempTable("order_items")


/*
hivecontext.sql('select * from products').show()
hivecontext.sql('select * from orders').show()
hivecontext.sql('select * from order_items').show()
*/

## If you only need few partitions for running this query
hivecontext.setConf("spark.sql.shuffle.partitions","2")

sql_cmd = "SELECT o.order_date,p.product_name, round(sum(oi.order_item_subtotal),2) as daily_revenue \
FROM orders o join order_items oi on (o.order_id = oi.order_item_order_id) \
join products p on (oi.order_item_product_id = p.product_id) \
where o.order_status in ('COMPLETE','CLOSED') \
group by o.order_date,p.product_name \
order by o.order_date asc,daily_revenue desc"

daily_revenue_per_product=hivecontext.sql(sql_cmd)

hivecontext.sql("CREATE DATABASE gmadhu89_daily_revenue")

##You can create a new table and Insert from Dataframe 
hivecontext.sql("CREATE TABLE gmadhu89_daily_revenue.daily_revenue (order_date string,product_name string,daily_revenue_per_product float)")
daily_revenue_per_product.write.insertInto("gmadhu89_daily_revenue.daily_revenue")

##OR - You can save as table directly from a DF
daily_revenue_per_product.write.mode("overwrite").saveAsTable("gmadhu89_daily_revenue.daily_revenue")