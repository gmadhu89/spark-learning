'''
orders = spark.read.csv("/public/retail_db/orders",sep=",",schema="order_id int, order_date string,order_customer_id int,order_status string")

orders_new = orders.withColumn("order_date_new",F.unix_timestamp(orders.order_date,'yyyy-MM-dd HH:mm:ss')).drop("order_date").withColumnRenamed("order_date_new","order_date")
orders_new.write.mode('overwrite').format("parquet").save("/user/gmadhu89/q7/input")

Find out all PENDING_PAYMENT orders in March 2014.
order_date format is in unix_timestamp

Output should be date and total pending order for that date.

Output should be saved at below hdfs location
/user/cloudera/practice1/question8/output
Output should be json file format.

'''

from pyspark.sql import functions as F

orders = spark.read.format("parquet").load("/user/gmadhu89/q7/input")

orders_map = orders.withColumn("order_date_new",F.from_unixtime(orders.order_date)).drop("order_date").withColumnRenamed("order_date_new","order_date")

orders_map.createOrReplaceTempView("orders")

sql_cmd = "select order_date,count(order_id) pending_orders from orders where substring(order_date,1,7) = '2014-03' and order_status = 'PENDING_PAYMENT'\
group by order_date \
order by order_date"
orders_filtered = spark.sql(sql_cmd)

orders_filtered.coalesce(4).write.mode("overwrite").json("/user/gmadhu89/q7/output/")