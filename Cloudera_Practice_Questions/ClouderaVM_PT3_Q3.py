'''
sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table orders --fields-terminated-by "\t" --target-dir /user/cloudera/practice3/problem3/orders
sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table order_items --fields-terminated-by "\t" --target-dir /user/cloudera/practice3/problem3/order_items 
sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table customers --fields-terminated-by "\t" --target-dir /user/cloudera/practice3/problem3/customers

Get count of customers in each city who have placed order of amount more than 100 and  whose order status is not PENDING.

Input files are tab delimeted files placed at below HDFS location:

"/user/gmadhu89/PT3/Q3/Input_orders"
"/user/gmadhu89/PT3/Q3/Input_orderitems"
"/user/gmadhu89/PT3/Q3/Input_customers"


Output Requirements:
Output should be placed in below HDFS Location
/user/cloudera/practice3/problem3/joinResults

Output file should be tab separated file with deflate compression.

[Providing the solution here only because answer is too long to put in choices.You will not be provided with any answer choice in actual exam.Below answer is just provided to guide you]
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice Test Question 3").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import col

orders = spark.read.format("csv").option("sep","\t").load("/user/gmadhu89/PT3/Q3/Input_orders").toDF("Order_id","order_date","order_customer_id","order_status")
order_items = spark.read.format("csv").option("sep","\t").load("/user/gmadhu89/PT3/Q3/Input_orderitems").toDF("Order_item_id","Order_item_order_id","order_item_product_id","Order_item_quantity","Order_item_subtotal","Order_item_product_price")
customers = spark.read.format("csv").option("sep","\t").load("/user/gmadhu89/PT3/Q3/Input_customers").toDF("Customer_id","customer_fname","customer_lname","customer_email","customer_password","customer_street","customer_city","customer_state","customer_zipcode")

ordersDF = orders.withColumn("Order_id",col('Order_id').cast(IntegerType())) \
.withColumn("order_customer_id",col('order_customer_id').cast(IntegerType())) \
.filter(~col('order_status').like('%PENDING%'))

customersDF = customers.withColumn("Customer_id",col('Customer_id').cast(IntegerType()))

order_items_DF = order_items.withColumn("Order_item_id",col('Order_item_id').cast(IntegerType())) \
.withColumn("Order_item_order_id",col('Order_item_order_id').cast(IntegerType())) \
.withColumn("Order_item_subtotal",col('Order_item_subtotal').cast(FloatType()))


ordersDF.createOrReplaceTempView("orders")
order_items_DF.createOrReplaceTempView("order_items")
customersDF.createOrReplaceTempView("customers")


sql_cmd = "select concat(c.customer_city,'\t',count(c.Customer_id)) as final \
from orders o, order_items oi, customers c \
where o.Order_id = oi.Order_item_order_id \
and o.order_customer_id = c.Customer_id \
group by c.customer_city,c.Customer_id \
having sum(oi.Order_item_subtotal) > 100"

cust_1 = spark.sql(sql_cmd)

cust_1.write.format("text").mode("overwrite").option("compression","deflate").save("/user/gmadhu89/PT3/Q3/Output")

'''
orders1 = spark.sql("select o.Order_id,oi.Order_item_subtotal,o.order_customer_id from orders o, order_items oi where o.Order_id = oi.Order_item_order_id \
and oi.Order_item_subtotal > 100")

orders1.createOrReplaceTempView("orders1")

cust_2 = spark.sql("select concat(c.customer_city,'\t',count(1)) as final from orders1 o, customers c \
where o.order_customer_id = c.Customer_id \
group by c.customer_city")
'''