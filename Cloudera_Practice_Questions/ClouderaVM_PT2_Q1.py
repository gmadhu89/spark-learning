'''

orders(Tab Delimited csv file) :  /user/gmadhu89/PT2/Q1Input
order_items(Tab Delimited csv file) :  /user/gmadhu89/PT2/Q1InputOI
customers(Tab Delimited csv file) :  /user/gmadhu89/PT2/Q1InputCust

Get all customers who have placed order of amount more than 200.
Input files are tab delimeted files placed at below HDFS location:

/user/cloudera/practice2/problem3/customers
/user/cloudera/practice2/problem3/orders
/user/cloudera/practice2/problem3/order_items	


Schema for customers File
Customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode
 
Schema for Orders File
Order_id,order_date,order_customer_id,order_status
 
Schema for Order_Items File
Order_item_id,Order_item_order_id,order_item_product_id,Order_item_quantity,Order_item_subtotal,Order_item_product_price

>> Output should be placed in below HDFS Location

/user/cloudera/practice2/problem3/joinResults

>> Output file should be comma seperated file with customer_fname,customer_lname,customer_city,order_amount.
>> Below header should be added to the output
fname, lname,city,price

'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice Test 2 Question 1").getOrCreate()
sc = sparkContext


orders = spark.read.csv("/user/gmadhu89/PT2/Q1Input",schema="Order_id int,order_date string,order_customer_id int,order_status string",sep="\t")
order_items = spark.read.csv("/user/gmadhu89/PT2/Q1InputOI",schema="Order_item_id int,Order_item_order_id int,order_item_product_id int,Order_item_quantity int,Order_item_subtotal float,Order_item_product_price float",sep="\t")
customers = spark.read.csv("/user/gmadhu89/PT2/Q1InputCust",schema="Customer_id int,customer_fname string,customer_lname string,customer_email string,customer_password string,customer_street string,customer_city string,customer_state string,customer_zipcode string",sep="\t")

orders.createOrReplaceTempView("orders")
order_items.createOrReplaceTempView("order_items")
customers.createOrReplaceTempView("customers")

sql_cmd = "select o.Order_id,sum(oi.Order_item_subtotal) as price \
from orders o , order_items oi \
where o.Order_id = oi.Order_item_order_id \
group by o.Order_id \
having sum(oi.Order_item_subtotal) > 200"

orders_more_200 = spark.sql(sql_cmd)

orders_more_200.createOrReplaceTempView("orders_more_200")

sql_cmd2 = "select c.customer_fname as fname,c.customer_lname as lname,c.customer_city as city,o2.price \
from orders_more_200 o2, customers c, orders o \
where o2.Order_id = o.Order_id and \
o.order_customer_id = c.customer_id"

cust_more_than_200 = spark.sql(sql_cmd2)

cust_more_than_200.write.format("csv").option("header","true").save("/user/gmadhu89/PT2/output")


'''
test=orders.join(order_items,orders.Order_id == order_items.Order_item_order_id,'inner').groupBy(orders.Order_id).agg({"Order_item_subtotal":"sum"}).withColumnRenamed("sum(Order_item_subtotal)","price").withColumnRenamed("Order_id","test_Order_id")
orders_more_200 = orders.join(test, orders.Order_id == test.test_Order_id,'inner').filter(col('price') > 200)
cust = customers.join(orders_more_200,customers.Customer_id==orders_more_200.order_customer_id,'inner').select("customer_fname","customer_lname","customer_city","price")
cust_final = cust.withColumnRenamed("customer_fname","fname").withColumnRenamed("customer_lname","lname")\
.withColumnRenamed("customer_city","city")

'''