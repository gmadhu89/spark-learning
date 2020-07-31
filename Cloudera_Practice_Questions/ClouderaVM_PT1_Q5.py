'''

customers = spark.read.csv("/public/retail_db/customers")
cust1 = customers.select("_c0","_c1","_c2").toDF(schema=["customer_id","customer_fname","customer_lname"])
cust.write.format("com.databricks.spark.avro").save("/user/gmadhu89/avro_source/q1")


Convert avro data-files stored at hdfs location /user/cloudera/practice1/problem7/customer/avro  into tab delimited file

Result should be saved in /user/cloudera/practice1/problem7/customer_text_bzip2
Output file should be saved as tab delimited file in bzip2 Compression.
Output should consist of customer_id   customer_name(only first three letter)   customer_lname

Sample Output:
21    And   Smith
111    Mar    Jons


'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Avro to file").getOrCreate()
sc = spark.sqlContext

customers = spark.read.format("com.databricks.spark.avro").load("/user/gmadhu89/avro_source/q1")

from pyspark.sql.functions import substring
cust = customers.withColumn('cust_nm_new',substring(customers.customer_fname,1,3))

cust_1 = cust.drop("customer_fname").withColumnRenamed("cust_nm_new","customer_fname")

cust_final = cust_1.select("customer_id","customer_fname","customer_lname")

from pyspark.sql.functions import concat,concat_ws,lit,col

cust_final2 = cust_final.withColumn("final",concat(col('customer_id'),lit('\t'),col('customer_fname'),lit('\t'),col('customer_lname'))) \
.drop('customer_id').drop('customer_fname').drop('customer_lname')

#cust_final.write.mode("overwrite").format("csv").option("sep","\t").option("compression","bzip2").save("/user/gmadhu89/avro_source/q1/output")
cust_final2.write.mode("overwrite").format("text").option("compression","bzip2").save("/user/gmadhu89/avro_source/q1/output")