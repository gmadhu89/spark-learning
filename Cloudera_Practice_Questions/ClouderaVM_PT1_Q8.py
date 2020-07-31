'''
orders = spark.read.csv("/public/retail_db/orders",sep=",",schema="order_id int, order_date string,order_customer_id int,order_status string")

orders_new = orders.withColumn("order_date_new",F.unix_timestamp(orders.order_date,'yyyy-MM-dd HH:mm:ss')).drop("order_date").withColumnRenamed("order_date_new","order_date")
orders_new.write.format("com.databricks.spark.avro").save("/user/gmadhu89/q8/input")

customers = spark.read.csv("/public/retail_db/customers")
customers.write.format("com.databricks.spark.avro").save("/user/gmadhu89/q8/input_cust")


Find out total number of orders placed by each customers in year 2013.
Order status should be COMPLETE
order_date format is in unix_timestamp

Input customer & order files are stored as avro file at below hdfs location
/user/cloudera/practice2/question8/orders
/user/cloudera/practice2/question8/customers

Output should be stored in a hive table named "customer_order" with three columns customer_fname,customer_lname and orders_count.
Hive tables should be partitioned by customer_state.


To enable Hive partitioning, please run these command in your spark-shell

spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict") 
'''

## pyspark2 --master yarn --conf spark.ui.port=12707 --packages com.databricks:spark-avro_2.11:4.0.0

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Question8').getOrCreate()
sc = spark.sparkContext

orders = spark.read.format("com.databricks.spark.avro").load("/user/gmadhu89/q8/input")
customers = spark.read.format("com.databricks.spark.avro").load("/user/gmadhu89/q8/input_cust")

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,from_unixtime

cust= customers.withColumnRenamed("_c0","customer_id").withColumnRenamed("_c1","customer_fname").withColumnRenamed("_c2","customer_lname")\
.withColumnRenamed("_c3","customer_email").withColumnRenamed("_c4","customer_pass").withColumnRenamed("_c5","customer_street") \
.withColumnRenamed("_c6","customer_city").withColumnRenamed("_c7","customer_state").withColumnRenamed("_c8","customer_zip") \
.withColumn("customer_id",col('customer_id').cast(IntegerType()))

orders1 = orders.withColumn("order_date",from_unixtime(orders.order_date)).withColumn("order_id",col('order_id').cast(IntegerType()))


orders1.createOrReplaceTempView("orders")
cust.createOrReplaceTempView("customers")

sql_cmd = "select c.customer_id,c.customer_fname,c.customer_lname,c.customer_state,count(o.order_id) as orders_count from orders o, customers c \
where o.order_customer_id = c.customer_id \
and o.order_status = 'COMPLETE' \
and year(o.order_date) = '2013' \
group by c.customer_id,c.customer_fname,c.customer_lname,c.customer_state"

orders_per_cust = spark.sql(sql_cmd)

final_result = orders_per_cust.drop("customer_id")

hiveContext.setConf("hive.exec.dynamic.partition", "true")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

#final_result.write.saveAsTable("gmadhu89_retail_db.customer_order",partitionBy='customer_state')
final_result.write.mode("overwrite").partitionBy("customer_state").saveAsTable("gmadhu89_retail_db.customer_order")




