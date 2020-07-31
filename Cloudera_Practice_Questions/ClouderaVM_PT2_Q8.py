'''



customers.select("_c0","_c1","_c6").withColumn("final",concat(col('_c0'),lit('\t'),col('_c1'),lit('\t'),col('_c6'))).select('final') \
.write.mode('overwrite').format("csv").save("/user/gmadhu89/PT2/q8/input")

sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table customers --fields-terminated-by '\t' --columns "customer_id,customer_fname,customer_city"  --target-dir /user/cloudera/problem6/customer/text
Find all customers that lives 'Brownsville' city and save the result into HDFS.

Input folder is  /user/cloudera/problem6/customer/text


Result should be saved in /user/cloudera/problem6/customer_Brownsville Output file should be saved in Json format

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


customers = spark.read.format("csv").option("sep","\t").load("/user/gmadhu89/PT2/q8/input")

customers.filter(customers._c2 == 'Brownsville').write.mode('overwrite').format("json").save("/user/gmadhu89/PT2/q8/output")