'''

sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table customers  --target-dir /user/cloudera/problem2/customer/tab --fields-terminated-by "|" --columns "customer_id,customer_fname,customer_state"

Data Setup:
customers = spark.read.csv("/public/retail_db/customers")
cust = customers.select("_c0","_c1","_c7").withColumnRenamed("_c0","customer_id").withColumnRenamed("_c1","customer_fname").withColumnRenamed("_c7","cust_state")
cust.write.option("sep","|").format("csv").save("/user/gmadhu89/PT2/q3/input")

Provided pipe delimited file, get total numbers customers in each state whose first name starts with 'M'   and  save results in HDFS.

Input folder
/user/cloudera/problem2/customer/tab

Result should be saved in a hive table "customer_m"
File format should be parquet file with gzip compression.
Output should have state name followed by total number of customers in that state.


1. In case you face table not found issue. Just check that SPARK_HOME/conf has hive_site.xml copied from /etc/hive/conf/hive_site.xml.
2. If in case any derby lock issue occurs, delete SPARK_HOME/metastore_db/dbex.lck   to release the lock.
3. You can check the meta information about parquet using parquet-tools meta command.

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice test 2 Question 3").getOrCreate()
sc = spark.sparkContext

customers = spark.read.format("csv").option("sep","|").load("/user/gmadhu89/PT2/q3/input").withColumnRenamed("_c0","customer_id") \
.withColumnRenamed("_c1","customer_fname").withColumnRenamed("_c2","state")

customers.createOrReplaceTempView("customers")

sql_cmd = "select state,count(customer_id) as total_no_cust \
from customers where customer_fname like 'M%' \
group by state"

cust_result = spark.sql(sql_cmd)

cust_result.write.saveAsTable("gmadhu89_retail_db.customer_m",format='parquet',mode='overwrite',compression='gzip')