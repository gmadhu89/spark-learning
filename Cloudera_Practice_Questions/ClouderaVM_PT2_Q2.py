'''

Get Customers from metastore table named "customers_hive" whose fname is like "Rich" and save the results in HDFS.

Result should be saved in /user/cloudera/practice2/problem4/customers/output.

Output should contain only fname, lname and city

Output should be saved in text format.

Output should be sorted by customer_city
fname and lname should seperated by tab with city seperated by colon

Richard Plaza:Francisco
Rich Smith:Chicago

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc= spark.sparkContext

from pyspark.sql.functions import concat,concat_ws,col,lit


customers = spark.sql("select * from gmadhu89_retail_db.customers where customer_fname like '%Rich%' order by customer_city") \
.withColumnRenamed("customer_fname","fname") \
.withColumnRenamed("customer_lname","lname").withColumnRenamed("customer_city","city")

customers_final = customers.withColumn("final",concat(col('fname'),lit('\t'),col('lname'),lit(':'),col('city'))).select("final")

customers_final.write.format("text").save("/user/gmadhu89/PT2/q2/output")

'''

customers2 = spark.sql("select concat(customer_fname,'\t',customer_lname,':',customer_city) as final from gmadhu89_retail_db.customers where customer_fname like '%Rich%' order by customer_city")
customers2.write.format("text").save("/user/gmadhu89/PT2/q2/output2")

'''