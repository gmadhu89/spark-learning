'''

sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table customers --fields-terminated-by '\t'   --target-dir /user/cloudera/problem3/all/customer/input
customers.write.format("csv").option("sep","\t").save("/user/gmadhu89/PT2/q6/input")

provided tab delimited file at hdfs location /user/cloudera/problem3/all/customer/input 
save only first 4 field in the result as pipe delimited file in HDFS

Result should be saved in /user/cloudera/problem3/all/customer/output
Output file should be saved in text format.

'''
from pyspark.sql import SparkSession
spark = spark.builder.appName('PT 2 Q6').getOrCreate()
sc = spark.sparkContext

from pyspark.sql.functions import concat,lit,concat_ws,col
customers = spark.read.format("csv").option("sep","\t").load("/user/gmadhu89/PT2/q6/input").select("_c0","_c1","_c2","_c3")

customers_result = customers.withColumn("final",concat(col('_c0'),lit("|"),col("_c1"),lit("|"),col("_c2"),lit("|"),col("_c3")))

customers_result.select('final').write.format("text").mode('overwrite').save("/user/gmadhu89/PT2/q6/output")