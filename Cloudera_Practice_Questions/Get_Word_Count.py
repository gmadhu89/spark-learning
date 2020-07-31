'''
Data is available in HDFS /public/randomtextwriter
Get word count for the input data using space as delimiter (for each word, we need to get how many types it is repeated in the entire input data set)
Number of executors should be 10
Executor memory should be 3 GB
Executor cores should be 20 in total (2 per executor)
Number of output files should be 8
Avro dependency details: groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
Target Directory: /user/<YOUR_USER_ID>/solutions/solution05/wordcount
Target File Format: Avro
Target fields: word, count
Compression: N/A or default


spark-submit --master yarn --conf spark.ui.port=12909 \
--executor-cores 2 \
--num-executors 6 \
--executor-memory 3G \
--packages 'com.databricks:spark-avro_2.10:2.0.1'

'''

from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("Get Word count")
sc=SparkContext(conf=conf)
from pyspark.sql import SQLContext
from pyspark import sql
sqlcontext=sql.SQLContext(sc)

from pyspark.sql.import Row

wordData=sc.sequenceFile("/public/randomtextwriter")
wordMap = wordData.map(lambda rec:rec[0]+" "+rec[1]).flatMap(lambda rec:str(rec.split(" "))).map(lambda rec:(rec,1))

wordGroup=wordMap.reduceByKey(lambda x,y:x+y)

wordDF=wordGroup.map(lambda rec: Row(str(rec[0]),int(rec[1]))).toDF(schema=["word","count"])

wordDF.coalesce(8).write.format('com.databricks.spark.avro') \
.save("/user/gmadhu89/solutions/solution05/wordcount")




