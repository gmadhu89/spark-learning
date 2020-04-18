### Query Dataframas as tables in Relational Database
## Register them as SQL Tables
##      Temporary or Global

## Catalyst Optimizer for SQL Queries - Execution is very fast

from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .appName("Analyzing airline data")\
                    .getOrCreate()
                    

from pyspark.sql.types import Row
from datetime import datetime

record = sc.parallelize([Row(id=1,
                            name = "Jill",
                            active=True,
                            clubs= ['chess','hockey'],
                            subjects = {"math":80,"english":56},
                            enrolled=datetime(2014,8,1,14,1,5)),
                        Row(id=2,
                            name="Jack",
                            active=False,
                            clubs= ['chess','soccer'],
                            subjects = {"math":100,"english":98},
                            enrolled=datetime(2015,8,1,14,1,5))
                        ])
                        
# Convert RDD to a Dataframe
record_df = record.toDF()
record_df.show()

##Register the Dataframe as a Temporary Tables
## Temp Table - Not shared accross sessions, just within this session
record_df.createOrReplaceTempView("records")  ## Name of the temp table

                     
all_records_df = sqlContext.sql('select * from records')
## Sql Context returns data as Dataframes

all_records_df.show()
sqlContext.sql('select id,clubs[1],subjects["english"] from records').show()

sqlContext.sql('SELECT ID, NOT active from records').show()

sqlContext.sql('select * from records where active').show()

sqlContext.sql('select * from records where subjects["english"] > 90').show()

## Global Temp table
record_df.createGlobalTempView('global_records')

##Should give specific namespace while accessing Global Temp Tables
##global_temp = namespace
sqlContext.sql('SELECT * FROM global_temp.global_records').show()

##Anayzing Airline Database
airlinesPath = "C:\Users\mganesan.CCCIS\Desktop\NGA\SparkLearning\data\airlines.csv"
flightsPath = "C:\Users\mganesan.CCCIS\Desktop\NGA\SparkLearning\data\flights.csv"
airportsPath = "C:\Users\mganesan.CCCIS\Desktop\NGA\SparkLearning\data\airports.csv"

airlines = spark.read\ 
                .format("csv")\
                .option("header","true")\
                .load(airlinesPath)
                
airlines.createOrReplaceTempView("airlines")

airlines = spark.sql('select * from airlines')
airlines.show()
airlines.columns

flights = spark.read\ 
                .format("csv")\
                .option("header","true")\
                .load(flightsPath)        
                
flights.createOrReplaceTempView("flights")

flights = spark.sql('select * from flights')
flights.show()
flights.columns                

flights.count() , airlines.count()

flights_count = spark.sql('SELECT COUNT(*) FROM flights')  ## Results are stored as Dataframes and need to be extracted
airlines_count = spark.sql('SELECT COUNT(*) FROM airlines')

flights_count.collect()[0][0] ,airlines_count.collect()[0][0]

total_distance_df = spark.sql("select distance from FLIGHTS")\
                         .agg({"distance":"sum"})\
                         .withColumnRenamed("sum(distance)","total_distance")
                         
total_distance_df.show()

all_delays_2014 = spark.sql (
                            "select date,airline,flight_number,departure_delay "+
                            "from flights where departure_delay > 0 and year(date) = 2014")

all_delays_2014.show(5)

all_delays_2014.createOrReplaceTempView("all_delays")

all_delays_2014.orderBy(all_delays_2014.departure_delay.desc()).show()

delay_count = spark.sql("select count(1) from all_delays")
delay_count.show()

delay_percent = delay_count.collect()[0][0]/flights_count.collect()[0][0] * 100
delay_percent

delay_per_airline = spark.sql("select airlines,departure_delay from flights")\
                         .groupBy("airlines")\
                         .agg({"departure_delay":"avg"})\
                         .withColumnRenamed("avg[departure_delay]":"departure_delay")

delay_per_airline.orderBy(delay_per_airline.departure_delay.desc()).show()


##Same result using a sql query
delay_per_airline.createOrReplaceTempView("delay_per_airline")
delay_per_airline = spark.sql ("SELECT * FROM delay_per_airline order by departure_delay desc")
delay_per_airline.show()

## To get names of airlines with highest departure delay

spark.sql("select * from delay_per_airline" +
           "join airlines on airlines.code = delay_per_airline.airlines "+
           "order by departure_delay desc").show()

##Inferred and Explicit Schema
##Import Students Data

from pyspark.sql import SparkSession

spark = SparkSession.builder\       
                    .appName("Inferred and Explicit Schema")\
                    .getOrCreate()

from pyspark.sql.types import Row

lines = sc.textFile("C:\Users\mganesan.CCCIS\Desktop\NGA\SparkLearning\data\students/txt")
lines.collect()

parts = lines.map(lamba l:l.split(","))
parts.collect()

##Convert every element to a row object
students = parts.map(lambda p: Row(name=p[0], math=int(p[1]), english = int(p[2]), science = int(p[3])))
students.collect()

schemaStudents = spark.createDataFrame(students)
schemaStudents.createOrReplaceTempView("students")

schemaStudents.columns
schemaStudents.schema ##Default created

##Explicitly  specify schema
parts.collect()

schemaString="name math english science"

from pyspark.sql import StructType,StructField,StringType.LongType

fields = [StructField('name', StringType(),True,
          StructField('math', LongType(),True,
          StructField('english', LongType(),True,
          StructField('science', LongType(),True
           ]
           
schema = StructType(fields)

SchemaStudents = spark.createDataFrame(parts, schema)

## Window Functions Using Spark DataFrames

from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .appName("Window Functions")\
                    .getorCreate()

products = spark.read\
                .format("csv")\
                .option("header","true")\
                .load('C:\Users\mganesan.CCCIS\Desktop\NGA\SparkLearning\data\products.csv')
                
products.show()

import sys
import pyspark.sql.window import Window
import pyspark.sql.functions as func

## Rank, Partition, And ORder
windowSpec1 = Window.partitionBy(products['category'])\
                    .orderBy(products['price'].desc())
price_rank = (func.rank().over(windowSpec1))

product_rank = products.select(
            products['product'],
            products['category'],
            products['price']
).withColumn('rank',func.rank().over(windowSpec1))       

product_rank.show()        

## Windows Spec with Frame
windowSpec2 = Window.partitionBy(products['category'])\
                    .orderBy(products['price'].desc())\
                    .rowsBetween(-1,0)
price_max = (func.max(products['price']).over(windowSpec2))


products.select(
            products['product'],
            products['category'],
            products['price'],
            price_max.alias("price_max").show()
##Windows Spec with sysmaxsize
            

windowSpec3 = Window.partitionBy(products['category'])\
                    .orderBy(products['price'].desc())\
                    .rangebetween(-sys.maxsize,sys.maxsize)
price_difference = (func.max(products['price']).over(windowSpec3)) - product['price']