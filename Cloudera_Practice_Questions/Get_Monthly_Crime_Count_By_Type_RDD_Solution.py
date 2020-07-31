'''
Execution Command
spark-submit --master yarn \
--conf spark.ui.port=12709 \
--executor-cores 2 \
--num-executors 6 \
--executor-memory 3G \
/home/gmadhu89/get_monthly_crimecnt_ByType_RDD.py
'''

from pyspark import SparkContext,SparkConf
conf=SparkConf().setAppName('Monthly Crime count by Type')
sc=SparkContext(conf=conf)



crimeData=sc.textFile('/public/crime/csv/crime_data.csv')

crimeDataHeader=crimeData.first()

crimeDataWithoutHeader = crimeData.filter(lambda rec: rec!= crimeDataHeader)

def getMonthYear(rec):
	return int(rec.split(",")[2].split(" ")[0].split("/")[2] + rec.split(",")[2].split(" ")[0].split("/")[0])

crimeDataFormatted = crimeDataWithoutHeader.map(lambda rec : ((getMonthYear(rec), rec.split(",")[5]), 1 ))
crimeTypeCountPerMonth = crimeDataFormatted.reduceByKey(lambda x,y : x+y)
crimeTypeCountFormatted = crimeTypeCountPerMonth.map(lambda rec: ( (rec[0][0], -rec[1]) , str(rec[0][0]) + "\t" + str(rec[1]) + "\t" + str(rec[0][1])))
crimeTypeCountSorted = crimeTypeCountFormatted.sortByKey()

crimeTypeFinal = crimeTypeCountSorted.map(lambda rec:rec[1])

codec="org.apache.hadoop.io.compress.GzipCodec"

import subprocess
path="/user/gmadhu89/solutions/solution01/crimes_by_type_by_month"
cmd = 'hdfs dfs -test -e {path}'.format(path=path)


if subprocess.call(["hdfs","dfs","-test","-e",path]) == 0:
	subprocess.call(["hadoop","fs","-rm","-R",path])

crimeTypeFinal.coalesce(2).saveAsTextFile("/user/gmadhu89/solutions/solution01/crimes_by_type_by_month",codec)