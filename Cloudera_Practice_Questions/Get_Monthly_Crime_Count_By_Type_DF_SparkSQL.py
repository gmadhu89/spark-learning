'''
Execution Command
spark2-submit --master yarn \
--conf spark.ui.port=12709 \
--executor-cores 2 \
--num-executors 6 \
--executor-memory 3G \
/home/gmadhu89/get_monthly_crimecnt_ByType_DF_SparkSQL.py


Steps
1) Create a Dataframe by loading contents from CSV file
2) Register as a GLobal temporary table
3) Use SQL to get the requested data
4) Convert DF to RDD
5) Store as text file
'''

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Monthly Crime Count By Type Dataframe SparkSQL solution').getOrCreate()
from pyspark.sql.functions import col,substring,concat
from pyspark.sql.types import IntegerType

crimeData=spark.read\
.format("csv")\
.option("header","true")\
.option("sep",",")\
.load("/public/crime/csv/crime_data.csv")

crimeDataFormatted = crimeData.select('Date','Primary Type'). \
withColumn('Date_New', concat(substring(crimeData.Date,7,4),substring(crimeData.Date,1,2)).cast(IntegerType()))

from pyspark.sql. import HiveContext
hive=HiveContext(spark)

crimeDataFormatted.createOrReplaceGlobalTempView('crimeDataFormatted')

sql_cmd = "select Date_New,`Primary Type`,count(*) as `Count_Per_Type` from global_temp.crimeDataFormatted \
group by Date_New,`Primary Type` \
order by Date_New,count(*) desc"

crimeDataFinalHive=hive.sql(sql_cmd)

##Converting DF into RDD
crimeDataFinalRDD = crimeDataFinalHive.select('Date_New','Count_Per_Type','Primary Type'). \
withColumnRenamed('Primary Type','Primary_Type').rdd

crimeDataFinalDelim=crimeDataFinalRDD.map(lambda rec: str(rec.Date_New) + "\t" + str(rec.Count_Per_Type) + "\t" + str(rec.Primary_Type))

import subprocess
path="/user/gmadhu89/solutions/solution01/crimes_by_type_by_monthDFSQL"
cmd = 'hdfs dfs -test -e {path}'.format(path=path)

if subprocess.call(["hdfs","dfs","-test","-e",path]) == 0:
	subprocess.call(["hadoop","fs","-rm","-R",path])

codec="org.apache.hadoop.io.compress.GzipCodec"
crimeDataFinalDelim.coalesce(2).saveAsTextFile("/user/gmadhu89/solutions/solution01/crimes_by_type_by_monthDFSQL",codec)


#hadoop fs -get /user/gmadhu89/solutions/solution02/test /home/gmadhu89/testDF
#hadoop fs -get /user/gmadhu89/solutions/solution01/crimes_by_type_by_month /home/gmadhu89/testRDD