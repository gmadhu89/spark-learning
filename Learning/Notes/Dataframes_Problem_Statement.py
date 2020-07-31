##############################  GET DAILY REVENUE PER PRODUCT ########################

from pyspark.sql import SparkSession
spark = SparkSession.builder \
 					.appName("Dataframes problem statement") \
 					.getOrCreate()
#orders=orderDF.withColumn('order_id',order_id.cast(IntegerType()))
#	          .withColumn('order_customer_id',order_customer_id.cast(IntegerType()))
ordersDF=spark.read.csv("/public/retail_db/orders/part-00000",sep=',',schema='order_id int,order_date string,order_customer_id int,order_status string')
from pyspark.sql.types import IntegerType,FloatType


orderItemsDF=spark.read.csv("/public/retail_db/order_items/part-00000",sep=',').toDF('order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price')
orderItems=orderItemsDF.withColumn('order_item_id',orderItemsDF.order_item_id.cast(IntegerType())) \
.withColumn('order_item_order_id',orderItemsDF.order_item_order_id.cast(IntegerType())) \
.withColumn('order_item_product_id',orderItemsDF.order_item_product_id.cast(IntegerType())) \
.withColumn('order_item_quantity',orderItemsDF.order_item_quantity.cast(IntegerType())) \
.withColumn('order_item_subtotal',orderItemsDF.order_item_subtotal.cast(FloatType())) \
.withColumn('order_item_product_price',orderItemsDF.order_item_product_price.cast(FloatType()))

ordersDF.printSchema()
orderItems.printSchema()


### Some simple transformations for practice
##Orders that are Completed or Closed
ordersFiltered=ordersDF.filter("order_status in ('COMPLETE','CLOSED')")

#ordersFiltered=ordersDF.filter((ordersDF.order_status == 'COMPLETE') | (ordersDF.order_status == 'CLOSED'))
#ordersFiltered=ordersDF.filter(ordersDF.order_status.isin('COMPLETE','CLOSED'))

##Orders that are Completed or Closed and placed in 2013 August
ordersFiltered=ordersDF.filter("order_status in ('COMPLETE','CLOSED') and substring(order_date,1,7) = '2013-08'")

#Dataframe native style
ordersDF.filter((ordersDF.order_status.isin('COMPLETE','CLOSED')) & (orders.order_date.like('2013-08%')))

## Getting Orders items where Order item subtotal <> orderItem quantity multiplied by OrderItem product price
from pyspark.sql.functions import round
orderItems.filter("order_item_subtotal != round(order_item_quantity*order_item_product_price,2)").show()

orderItems.select('order_item_quantity','order_item_subtotal','order_item_product_price')\
.filter(orderItems.order_item_subtotal != round((orderItems.order_item_product_price * orderItems.order_item_quantity) ,2)).show()

##Get all orders placed in first of every month
ordersDF.filter("substring(order_date,9,2)='01'").show()
#(OR)
from pyspark.sql.functions import date_format
ordersDF.filter(date_format(ordersDF.order_date,'dd') == '01').show()


##Get all orderitems corresponding to completed or closed orders
ordersFiltered=ordersDF.filter("order_status in ('COMPLETE','CLOSED')").select('order_id','order_status')
ordersFiltered2=ordersFiltered.withColumn('order_id',ordersFiltered.order_id.cast(IntegerType()))
orderItemsFiltered=orderItems.join(ordersFiltered2,orderItems.order_item_order_id == ordersFiltered2.order_id,'inner')
##Get orders where there are no corresponding order items
orders_No_Items=ordersDF.join(orderItems,ordersDF.order_id == orderItems.order_item_order_id,'left'). \
filter("order_item_order_id is null")
##Check if there are any order items where there is no corresponding order in orders data set
ordersItems_No_Orders=ordersDF.join(orderItems,ordersDF.order_id == orderItems.order_item_order_id,'right'). \
filter("order_id is null")

## Get count of orders per status
ordersDF. groupBy('order_status').count().show()
from pyspark.sql.functions import count
ordersDF.groupBy('order_status') \
... .agg(count('order_status').alias('status_count')) \
... .show()

## Get the total revenue per date, produc_id
ordersJoin = ordersDF.join \
... (orderItems, ordersDF.order_id == orderItems.order_item_order_id)

from pyspark.sql.functions import sum,round
>>> ordersGroup = ordersJoin. \
groupBy('order_date','order_item_product_id'). \
agg(round(sum('order_item_subtotal'),2).alias('total'))

ordersGroupSorted = ordersGroup.sort(['order_date','total'],ascending=[1,0]).withColumnRenamed('total','revenue')


##Sorting the data
ordersDF.sort(['order_date','order_customer_id'],ascending=[1,0]).show()

ordersDF.sort(ordersDF.order_date.asc(),ordersDF.order_customer_id.desc()).show()

ordersDF.sortWithinPartitions(ordersDF.order_date,ordersDF.order_customer_id.desc()).show()
##Groups data on the higher order fieds and sorts the data based on the lower order field