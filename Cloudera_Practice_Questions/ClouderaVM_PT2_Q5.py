'''

sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table orders --as-parquetfile --target-dir /user/cloudera/problem3/parquet

orders.write.format('parquet').save("/user/gmadhu89/PT2/q5/Input")

Fetch all pending orders from  data-files stored at hdfs location /user/cloudera/problem3/parquet and save it  into json file  in HDFS
Result should be saved in /user/cloudera/problem3/orders_pending
Output file should be saved as json file.

Output file should Gzip compressed.


'''
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PT2 Question 5").getOrCreate()
sc = sprak.sparkContext

orders = spark.read.parquet("/user/gmadhu89/PT2/q5/Input").toDF("order_id","order_date","order_customer_id","order_status")
from pyspark.sql.functions import col
orders.filter(col('order_status').like('%PENDING%')).write.format('json').mode('overwrite').option("compression","gzip").save("/user/gmadhu89/PT2/Q5/output")
