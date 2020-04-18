from pyspark.sql.types import Row
from datetime import datetime

## Study Documentation Link
#http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=parallelize#pyspark.SparkContext

## SparkContext - Establishes a Connection to SparkCluster

sc = SparkContext(conf=conf)

## This will generate an RDD of the list of elements defined. if partitions are not provided, default partitions are taken up
simple_data = sc.parallelize([1,'Alice',50])    ## Initializing an RDD
simple_data   ##Type RDD (Resilient Distributed Dataset)

#################################################### Some METHODS operable on RDD's ##############################################################################
simple_data.count()     ## Gets number of elemets in RDD
simple_data.take(2)     ##Data is loaded to driver memory so run this only if data is small
simple_data.collect()   ## Collects and Displays all elements in RDD
simple_data.first()     ## Displays the first element of RDD. Gives an error if RDD is empty.

##lambda   - Similar to Python. Adhoc function that can be used on any RDD
## filter - Returns the result if the filter condition is satisfied
sc.parallelize([1,2,3,4,5]).filter(lambda x:x%2==0).collect()
sorted(sc.parallelize([1,2,3,4,5]).collect())
    ##[1, 2, 3, 4, 5]
##distinct() - gets the distinct values from the RDD    
sorted(sc.parallelize([1,5,6,3,1,1,6]).distinct().collect())
       #[1, 3, 5, 6]

##getNumPartitions() - Returns the number of partitions on RDD       
rdd=sc.parallelize([1,2,3,4,5,6])
rdd.getNumPartitions()       


##getStorageLevel() Returns the RDD's storage level
rdd1 = sc.parallelize([1,2])
>>> rdd1.getStorageLevel()
StorageLevel(False, False, False, False, 1)
>>> print(rdd1.getStorageLevel())
Serialized 1x Replicated

#isEmpty() -- Returns Treu if RDD is empty, else Returns False
>>> sc.parallelize([]).isEmpty()
True
>>> sc.parallelize([1]).isEmpty()
False

##map() on RDD
>>> rdd=sc.parallelize(['b','a','c'])
>>> rdd.map(lambda x:(x,1)).collect()
[('b', 1), ('a', 1), ('c', 1)]
>>> sorted(rdd.map(lambda x:(x,1)).collect())
[('a', 1), ('b', 1), ('c', 1)]

##sum() on RDD
sc.parallelize([1.0, 2.0, 3.0]).sum()
#6.0

## top() - Returns the elements in descending order . This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory.
>>> sc.parallelize([10, 4, 2, 12, 3]).top(1)
[12]
>>> sc.parallelize([2, 3, 4, 5, 6], 2).top(2)
[6, 5]
>>> sc.parallelize([10, 4, 2, 12, 3]).top(3, key=str)
[4, 3, 2]
>>>
############################################################################################################################################################


records = sc.parallelize([[1,'Alice',50],[2,'Bob',25]])
records.count()
records.collect()
records.take(1)

## Convert an RDD to a Dataframe
df=records.toDF()
df.show()   ## Diplays like a table

##To give specific column names

data = sc.parallelize([Row(id=1,name='Alice',score=50)
					  , Row(id=2,name='Bob',score=70)
					  ,Row(id=3,name='Janice',score=2000)
					  ]
					  )		-- Row objects

df = data.toDF()

>>> data = sc.parallelize([Row(Id=1,Name='Alice',Score=50),Row(Id=2,Name='Bob',Score=75),Row(Id=3,Name='Janice',Score=100)])
>>> data.toDF().show()
+---+------+-----+
| Id|  Name|Score|
+---+------+-----+
|  1| Alice|   50|
|  2|   Bob|   75|
|  3|Janice|  100|
+---+------+-----+

## Complex DataFrames
complex_data = sc.parallelize([
								Row(
								col_list = [1,2,3],
								col_dict = {"k1":0},
								col_row = Row(a=10,b='Name',c=50.5),
								col_time = datetime(2014,8,1,14,1,5)
								)
								,Row(
								col_list = [4,5,6],
								col_dict = {"k2":5},
								col_row = Row(a=50,b='Name1',c=70.02),
								col_time = datetime(2014,8,1,14,2,6)
								)
								,Row(
								col_list = [7,8,9],
								col_dict = {"k3":6},
								col_row = Row(a=60,b='Name2',c=100.78),
								col_time = datetime(2014,9,1,14,3,7)
								)						
							])
                            

>>> complex_data = sc.parallelize([
...                                                             Row(
...                                                             col_list = [1,2,3],
...                                                             col_dict = {"k1":0},
...                                                             col_row = Row(a=10,b='Name',c=50.5),
...                                                             col_time = datetime(2014,8,1,14,1,5)
...                                                             )
...                                                             ,Row(
...                                                             col_list = [4,5,6],
...                                                             col_dict = {"k2":5},
...                                                             col_row = Row(a=50,b='Name1',c=70.02),
...                                                             col_time = datetime(2014,8,1,14,2,6)
...                                                             )
...                                                             ,Row(
...                                                             col_list = [7,8,9],
...                                                             col_dict = {"k3":6},
...                                                             col_row = Row(a=60,b='Name2',c=100.78),
...                                                             col_time = datetime(2014,9,1,14,3,7)
...                                                             )
...                                                     ])
>>>
>>> complex_data.toDF().show()
+---------+---------+-------------------+-------------------+
| col_dict| col_list|            col_row|           col_time|
+---------+---------+-------------------+-------------------+
|[k1 -> 0]|[1, 2, 3]|   [10, Name, 50.5]|2014-08-01 14:01:05|
|[k2 -> 5]|[4, 5, 6]| [50, Name1, 70.02]|2014-08-01 14:02:06|
|[k3 -> 6]|[7, 8, 9]|[60, Name2, 100.78]|2014-09-01 14:03:07|
+---------+---------+-------------------+-------------------+


## SQL Funtionality within Spark                            
sqlcontext = SQLContext(sc)

df = sqlcontext.range(5)    -- Generates a Dataframe
df.show()
df.count()

data = [('Alice',50),('Bob',60),('Janice',70)]
sqlcontext.createDataFrame(data).show()

## to display with column names
sqlcontext.createDataFrame(data,['Name','Score']).show()  

Complex_data = [(1.0,1,'Alice',True,[1,2,3],{"k1":0},Row(a=1,b=2,c=3),datetime(2014,9,1,14,3,7))
				,(1.56,20,'Bob',False,[10,8,15],{"k2":10},Row(a=10,b=20,c=30),datetime(2014,9,1,14,3,7))
				,(0.46,6000,'Janice',True,[100,0,10],{"k3":0.5},Row(a=19,b=29,c=39),datetime(2014,9,1,14,3,7))					
                ]

sqlcontext.createDataFrame(Complex_data).show()              
sqlcontext.createDataFrame(Complex_data,['col_float','col_int','col_str','col_bool','col_list','col_dict','col_row','col_date']).show()
Complex_data_df = sqlcontext.createDataFrame(Complex_data,['col_float','col_int','col_str','col_bool','col_list','col_dict','col_row','col_date'])

#################### Actual results : START##################################################################
 
>>> Complex_data.toDF().show()
+----+----+------+-----+------------+----------+------------+-------------------+
|  _1|  _2|    _3|   _4|          _5|        _6|          _7|                 _8|
+----+----+------+-----+------------+----------+------------+-------------------+
| 1.0|   1| Alice| true|   [1, 2, 3]| [k1 -> 0]|   [1, 2, 3]|2014-09-01 14:03:07|
|1.56|  20|   Bob|false| [10, 8, 15]|[k2 -> 10]|[10, 20, 30]|2014-09-01 14:03:07|
|0.46|6000|Janice| true|[100, 0, 10]|   [k3 ->]|[19, 29, 39]|2014-09-01 14:03:07|
+----+----+------+-----+------------+----------+------------+-------------------+

>>> sqlcontext.createDataFrame(Complex_data)
DataFrame[_1: double, _2: bigint, _3: string, _4: boolean, _5: array<bigint>, _6: map<string,bigint>, _7: struct<a:bigint,b:bigint,c:bigint>, _8: timestamp]
>>> sqlcontext.createDataFrame(Complex_data).show()
+----+----+------+-----+------------+----------+------------+-------------------+
|  _1|  _2|    _3|   _4|          _5|        _6|          _7|                 _8|
+----+----+------+-----+------------+----------+------------+-------------------+
| 1.0|   1| Alice| true|   [1, 2, 3]| [k1 -> 0]|   [1, 2, 3]|2014-09-01 14:03:07|
|1.56|  20|   Bob|false| [10, 8, 15]|[k2 -> 10]|[10, 20, 30]|2014-09-01 14:03:07|
|0.46|6000|Janice| true|[100, 0, 10]|   [k3 ->]|[19, 29, 39]|2014-09-01 14:03:07|
+----+----+------+-----+------------+----------+------------+-------------------+

>>> sqlcontext.createDataFrame(Complex_data,['col_float','col_int','col_str','col_bool','col_list','col_dict','col_row','col_date']).show()
+---------+-------+-------+--------+------------+----------+------------+-------------------+
|col_float|col_int|col_str|col_bool|    col_list|  col_dict|     col_row|           col_date|
+---------+-------+-------+--------+------------+----------+------------+-------------------+
|      1.0|      1|  Alice|    true|   [1, 2, 3]| [k1 -> 0]|   [1, 2, 3]|2014-09-01 14:03:07|
|     1.56|     20|    Bob|   false| [10, 8, 15]|[k2 -> 10]|[10, 20, 30]|2014-09-01 14:03:07|
|     0.46|   6000| Janice|    true|[100, 0, 10]|   [k3 ->]|[19, 29, 39]|2014-09-01 14:03:07|
+---------+-------+-------+--------+------------+----------+------------+-------------------+

#################### Actual results : END##################################################################

###Alternative

data = sc.parallelize([Row(1,'Alice',50)
                        ,Row(2,'Bob',500)
                        ,Row(3,'Janice',100)])
                        
column_names = Row('id','name','score')
students = data.map(lambda r: column_names(*r))

students.collect()
students_df = sqlcontext.createDataFrame(students)

#################### Actual results : START##################################################################
>>> data = sc.parallelize([Row(1,'Alice',50),Row(2,'Bob',500),Row(3,'Janice',100)])
>>> column_names = Row('id','name','score')
>>>
>>> students=data.map(lambda r:column_names(*r))
>>> students.toDF().show()
+---+------+-----+
| id|  name|score|
+---+------+-----+
|  1| Alice|   50|
|  2|   Bob|  500|
|  3|Janice|  100|
+---+------+-----+

>>> sqlcontext.createDataFrame(students).show()
+---+------+-----+
| id|  name|score|
+---+------+-----+
|  1| Alice|   50|
|  2|   Bob|  500|
|  3|Janice|  100|
+---+------+-----+

#################### Actual results : END##################################################################

Complex_data_df.first()
Complex_data_df.take(2)

Complex_data_df.collect()[0][2]

Complex_data_df.rdd\
                    .map(lambda x: (x.col_str,x.col_dict))\
                    .collect()
#################### Actual results : START##################################################################
>>> Complex_data_df.rdd\
...                     .map(lambda x: (x.col_str,x.col_dict))\
...                     .collect()
[('Alice', {'k1': 0}), ('Bob', {'k2': 10})]
#################### Actual results : END##################################################################                    

Complex_data_df.select('col_float','col_int','col_str').show()


Complex_data_df.rdd\
                .map(lambda x: (x.col_str + "Boo "))\
                .collect()
                
                
Complex_data_df.select('col_float','col_int')\
                .withColumn(
                    "col_sum",
                    Complex_data_df.col_int + Complex_data_df.col_float
                )\
                .show()
                
                
                
Complex_data_df.select('col_bool')\
                .withColumn(
                    "col_bool_ooposite",
                    Complex_data_df.col_bool == False
                )\
                .show()        

Complex_data_df.withColumnRenames('col_dict','col_dict_rename').show()               

Complex_data_df.select(Complex_data_df.col_str.alias("Name")).show()


import pandas
df_pandas = Complex_data_df.toPandas()

##Pandas Dataframes - Are in Memory (single machine)
##Spark Dataframes are built on top of RDD's - they are distributed acorss multiple nodes

df_spark = sqlcontext.createDataFrame(df_pandas).show()
####################################################################################################################################