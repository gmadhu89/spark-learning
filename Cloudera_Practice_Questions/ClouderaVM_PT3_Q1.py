'''
sqoop import  --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table orders  --warehouse-dir "/user/hive/warehouse/" --compress --compression-codec snappy --as-avrodatafile
sqoop import  --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table customers  --warehouse-dir "/user/hive/warehouse/" --compress --compression-codec snappy --as-avrodatafile


From the provided avro files at below HDFS location

orders.write.format("com.databricks.spark.avro").option("compression","snappy").save("/user/gmadhu89/PT3/Q1/Input_Orders")
customers.write.format("com.databricks.spark.avro").option("compression","snappy").save("/user/gmadhu89/PT3/Q1/Input_Customers")


Find out customers who have not placed any order in March 2014.

Output should be stored in json format at below HDFS location

/user/cca/practice3/ques1
Output should have two fields customer_fname:customer_lname and customer_city:customer_state.

{"name":"Melissa:Palmer","place":"Caguas:PR"}
{"name":"Mary:Rush","place":"Painesville:OH"}
{"name":"Thomas:Smith","place":"Caguas:PR"}
{"name":"Roger:Black","place":"Morrisville:PA"}

pyspark2 --master yarn --conf spark.ui.port=12709 --packages com.databricks:spark-avro_2.11:4.0.0

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice Test 3 Question 1").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.functions import col,substring,concat
from pyspark.sql.types import IntegerType, FloatType, StringType

orders = spark.read.format("com.databricks.spark.avro").load("/user/gmadhu89/PT3/Q1/Input_Orders")
customers = spark.read.format("com.databricks.spark.avro").load("/user/gmadhu89/PT3/Q1/Input_Customers")

ordersDF = orders.withColumn("order_id",col('_c0').cast(IntegerType())) \
.withColumn("order_date",col('_c1').cast(StringType())) \
.withColumn("order_customer_id",col('_c2').cast(IntegerType())) \
.withColumn("order_status",col('_c3').cast(StringType())) \
.drop('_c0').drop('_c1').drop('_c2').drop('_c3')


customersDF = customers.withColumn("customer_id",col('_c0').cast(IntegerType())) \
.withColumnRenamed('_c1',"customer_fname") \
.withColumnRenamed('_c2',"customer_lname") \
.withColumnRenamed('_c6',"customer_city") \
.withColumnRenamed('_c7',"customer_state") \
.drop('_c0')

ordersDF2 = ordersDF.withColumn("Date_New",substring(ordersDF.order_date,1,7)).filter(col('Date_New') == '2014-03')

#final = customersDF.join(ordersDF2, customersDF.customer_id == ordersDF2.order_customer_id, 'left_outer')
#CustNoOrders = customersDF.join(ordersDF2, customersDF.customer_id == ordersDF2.order_customer_id,'left_outer').filter(col('Date_New') == '2014-03') \
#.filter("order_id is null")


ordersDF2.createOrReplaceTempView("orders")
customersDF.createOrReplaceTempView("customers")

sql_cmd = "select concat(customer_fname,':',customer_lname) as name,concat(customer_city,':',customer_state) as place,order_customer_id from \
customers c left outer join orders o \
on (c.customer_id = o.order_customer_id) \
where o.order_customer_id is NULL"

cust_noorders_2014 = spark.sql(sql_cmd).drop('order_customer_id')

cust_noorders_2014.write.format("json").mode('overwrite').save("/user/gmadhu89/PT3/Q1/Ouput")