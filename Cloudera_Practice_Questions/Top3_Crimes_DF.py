'''
Get top 3 crimes based on number of incidents in RESIDENCE area

Data is available in HDFS file system under 
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community 
Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,” (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma 
and enclosed using double quotes.
Get top 3 crime types based on number of incidents in RESIDENCE area using “Location Description”
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA
Output Fields: Crime Type, Number of Incidents
Output File Format: JSON
Output Delimiter: N/A
Output Compression: No


spark2-submit --master yarn --conf spark.ui.port=12909 \
--executor-cores 2 --num-executors 6 --executor-memory 3G \
/home/gmadhu89/Top3_Crimes_DF.py

'''

from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("Top 3 Crimes")
sc=SparkContext(conf=conf)
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Top 3 Crimes").getOrCreate()
from pyspark.sql.functions import col
from pyspark.window.sql import *

crimeData=spark.read.format('csv') \
.option("sep",",") \
.option("header","true") \
.load("/public/crime/csv/crime_data.csv")

crimeDataFiltered = crimeData.withColumnRenamed('Location Description','Location_Description').filter(col('Location_Description') == "RESIDENCE") \
.select(col('ID'),col('Primary Type')).withColumnRenamed('Primary Type','Primary_Type')

crimeDataFiltered.createOrReplaceTempView('crimeDataFiltered')

sql_cmd = "select Primary_Type as Crime_type,count(distinct ID) as Number_Of_Incidents from crimeDataFiltered group by Primary_Type \
order by count(distinct ID) desc limit 3"	

crimeDataGroup = spark.sql(sql_cmd)

crimeDataGroupRDD = crimeDataGroup.rdd
Final = crimeDataGroupRDD.top(3,key=lambda rec:rec.Number_Of_Incidents)
FinalResult=sc.parallelize(Final).toDF(schema=["Crime_Type","Number_Of_Incidents"])

FinalResult.coalesce(1).write.json("/user/gmadhu89/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA_DF",'overwrite')
