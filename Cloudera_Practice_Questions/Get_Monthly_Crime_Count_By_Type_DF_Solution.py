'''
Execution Command
spark-submit --master yarn \
--conf spark.ui.port=12709 \
--executor-cores 2 \
--num-executors 6 \
--executor-memory 3G \
get_monthly_crimecnt_ByType_DF.py


Steps
1) Create a Dataframe by loading contents from CSV file
2) Register as a GLobal temporary table
3) Use SQL to get the requested data
4) Convert DF to RDD
5) Store as text file
'''

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Monthly Crime Count By Type Dataframe solution').getOrCreate()
from pyspark.sql.functions import col,substring,concat
from pyspark.sql.types import IntegerType

crimeData=spark.read\
.format("csv")\
.option("header","true")\
.option("sep",",")\
.load("/public/crime/csv/crime_data.csv")

crimeDataFormatted = crimeData.select('Date','Primary Type'). \
withColumn('Date_New', concat(substring(crimeData.Date,7,4),substring(crimeData.Date,1,2)).cast(IntegerType()))

crimeDataAgg = crimeDataFormatted.groupBy('Date_New','Primary Type').agg({'Primary Type':'count'}).\
withColumnRenamed('count(Primary Type)','Count_Per_Month_Type'). \
orderBy(col('Date_New'),col('Count_Per_Month_Type').desc())

import subprocess
path="/user/gmadhu89/solutions/solution01/crimes_by_type_by_monthDF"
cmd = 'hdfs dfs -test -e {path}'.format(path=path)

if subprocess.call(["hdfs","dfs","-test","-e",path]) == 0:
        subprocess.call(["hadoop","fs","-rm","-R",path])

crimeDataAgg.select('Date_New','Count_Per_Month_Type','Primary Type'). \
coalesce(2).write.option("delimiter","\t").csv("/user/gmadhu89/solutions/solution01/crimes_by_type_by_monthDF",compression="gzip")