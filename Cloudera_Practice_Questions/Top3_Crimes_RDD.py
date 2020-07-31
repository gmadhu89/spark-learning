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


spark-submit --master yarn --conf spark.ui.port=12909 \
--executor-cores 2 --num-executors 6 --executor-memory 3G \
/home/gmadhu89/Top3_Crimes_RDD.py

'''

from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("Top 3 Crimes")
sc=SparkContext(conf=conf)
from pyspark.sql import SQLContext
from pyspark import sql
sqlContext=sql.SQLContext(sc)

crimeData = sc.textFile("/public/crime/csv/crime_data.csv")

crimeHeader=crimeData.first()
crime=crimeData.filter(lambda rec:rec!=crimeHeader)

import re
regex = re.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

crimeResidence=crime.filter(lambda rec: regex.split(rec)[7]=='RESIDENCE')
crimeByType=crimeResidence.map(lambda rec : (str(regex.split(rec)[5]), str(regex.split(rec)[1])  ) )

#Crimecount=crimeByType.countByKey()  -->Gives Dict, which again has to be converted to RDD for sorting , so go by GroupBy Key

CrimeGroup = crimeByType.groupByKey().map(lambda rec: (rec[0], len(set(list(rec[1])))))
CrimeGroupMap=CrimeGroup.map(lambda rec: ( -int(rec[1]), rec ))
CrimeSorted=CrimeGroupMap.sortByKey().map(lambda rec:rec[1])

CrimeTop3=CrimeSorted.top(3,key = lambda rec:rec[1])
CrimeTop3Final=sc.parallelize(CrimeTop3).toDF(schema=["Crime_Type","Number_Of_Incidents"])

CrimeTop3Final.coalesce(1).write.json("/user/gmadhu89/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA",'overwrite')