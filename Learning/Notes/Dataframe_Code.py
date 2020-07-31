from pyspark.sql import SparkSession
spark = SparkSession.builder(). \
					appName("Test Spark Session"). \
					getOrCreate()

#Creating a Dataframe by reading a csv file and forcing the schema
ordersDF1=spark.read.csv("/public/retail_db/orders/part-00000",sep=',',schema='order_id int,order_date string,order_customer_id int,order_status string')

ordersDF1.printSchema()
#Not forcing schema, just defining column names
ordersDF2=spark.read.csv("/public/retail_db/orders/part-00000",sep=',') \
		  .toDF('order_id','order_date','order_customer_id','order_status')
ordersDF2.printSchema()

#To type cast Schema
from pyspark.sql.types import IntegerType
ordersDF2.select(ordersDF2.order_id.cast("int"),ordersDF2.order_date,ordersDF2.order_customer_id.cast(IntegerType()),ordersDF2.order_status)
#(OR)
ordersDF2.withColumn('order_id',ordersDF2.order_id.cast("int")) \
		 .withColumn('order_customer_id',ordersDF2.order_customer_id.cast("int"))

ordersDF.printSchema()


ordersDF3=spark.read.format('csv') \
			  .option(sep=',') \
			  .schema('order_id int,order_date string,order_customer_id int,order_status string') \
			  .load("/public/retail_db/orders/part-00000")   ##This is not need for JDBC

ordersDF3.printSchema()

ordersDF.printSchema()  ##This will display the schema related information


#Creating a Dataframe by reading a fixed width file

ordersFW=spark.read.text("/public/retail_db/orders/part-00000")