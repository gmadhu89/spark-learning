'''

sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table customers  --target-dir /user/cloudera/prac3/ques6/xml

Input file is provided at below HDFS location

/user/cloudera/prac3/ques6/xml

Append first three character of firstname with first two character of last name with a colon.


Output should be saved in xml file with rootTag as persons and rowTag as person.

Output should be saved at below HDFS location

/user/cloudera/prac3/ques6/output/xml

pyspark2 --master yarn --conf spark.ui.port=12709 --packages com.databricks:spark-xml_2.11:0.9.0

'''
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice Test Question 3").getOrCreate()
sc = spark.sparkContext

customers = spark.read.csv("/public/retail_db/customers")

from pyspark.sql.functions import substring,concat,lit

cust_result = customers.withColumn("Final",concat(substring('_c1',1,3),lit(':'),substring('_c2',1,2))).select('Final')

cust_result.write.mode('overwrite').format("com.databricks.spark.xml").option("rootTag","persons").option("rowTag","person").save("/user/gmadhu89/PT3/Q6/Output")