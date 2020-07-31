'''

sqoop import --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table order_items --target-dir /user/cloudera/practice4_ques6/order_items
 
sqoop import --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table products --target-dir /user/cloudera/practice4_ques6/products/ -m 1

Find top 10 products which has made highest revenue. Products and order_items data are placed in HDFS directory 
/user/cloudera/practice4_ques6/order_items/ and /user/cloudera/practice4_ques6/products/  respectively. 

Output should have product_id and revenue seperated with ':'  and should be saved in /user/cloudera/practice4_ques6/output

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import IntegerType,FloatType,StringType
from pyspark.sql.functions import col

order_items = spark.read.format("csv").option("sep",",").load("/public/retail_db/order_items").toDF('order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price')
products = spark.read.format("csv").option("sep",",").load("/public/retail_db/products").toDF('product_id','product_category_id','product_name','product_description','product_price','product_image')

oi = order_items.withColumn("order_item_id",col('order_item_id').cast(IntegerType())) \
.withColumn("order_item_order_id",col('order_item_order_id').cast(IntegerType())) \
.withColumn("order_item_product_id",col('order_item_product_id').cast(IntegerType())) \
.withColumn("order_item_subtotal",col('order_item_subtotal').cast(FloatType())) \
.withColumn("order_item_product_price",col('order_item_product_price').cast(FloatType()))

prd = products.withColumn("product_id",col('product_id').cast(IntegerType())) \
.withColumn("product_category_id",col('product_category_id').cast(IntegerType())) \
.withColumn("product_price",col('product_price').cast(FloatType()))

oi.createOrReplaceTempView("order_items")
prd.createOrReplaceTempView("products")

sql_cmd = "select concat(p.product_id,':',sum(oi.order_item_subtotal)) as output from order_items oi, products p \
where oi.order_item_product_id = p.product_id \
group by p.product_id \
order by sum(oi.order_item_subtotal) desc \
limit 10"

top_10_products = spark.sql(sql_cmd)

top_10_products.write.format("text").save("/user/gmadhu89/PT2/q7/output")