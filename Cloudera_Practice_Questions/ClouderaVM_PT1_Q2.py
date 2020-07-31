'''
sqoop import --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table customers --target-dir /user/cloudera/p1/p4/customers

sqoop import --connect "jdbc:mysql://localhost/retail_db" --username root --password cloudera --table orders --target-dir /user/cloudera/p1/p4/orders

Join the comma separated file located at hdfs location /user/cloudera/p1/p4/orders & /user/cloudera/p1/p4/customers to find out  
customers who have placed more than 4 orders.

Schema for customer File 
customer_id,customer_fname,......................................................
 
Schema for Order File 
order_id,order_date,order_customer_id,order_status

Order status should be COMPLETE

Output should have customer_id,customer_fname,count

Save the results in json format.

Result should be order by count of orders in ascending fashion.

Result should be saved in /user/cloudera/p1/p4/output 
'''

##pyspark2 --master yarn --executor-cores 2 --num-executors 6 --executor-memory 3G

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Customer with more than 4 orders").getOrCreate()
sc = spark.sparkContext

from pyspark.sql import functions as F

orders = spark.read.csv("/user/cloudera/p1/p4/orders",sep=",",schema="order_id int,order_date string,order_customer_id int,order_status string")

customers = spark.read.format("csv").option("sep",",") \
.load("/user/cloudera/p1/p4/customers") \
.toDF('customer_id','customer_fname','customer_lname','_c3','_c4','_c5','_c6','_c7','_c8')

ordersComplete = orders.filter(orders.order_status == 'COMPLETE').select('order_id','order_customer_id','order_status')

ordersComplete.createOrReplaceTempView("ordersComplete")
customers.createOrReplaceTempView("customers")

sql_cmd = "select customer_id,customer_fname,count(o.order_id) as count from customers c , ordersComplete o \
where c.customer_id = o.order_customer_id \
group by customer_id,customer_fname \
having count(o.order_id) > 4 \
order by count asc"

CustomersMoreThan4Orders = spark.sql(sql_cmd)

CustomersMoreThan4Orders.coalesce(2).write.json("/user/cloudera/p1/p4/output/",mode='overwrite')



'''

1)   Solution with DF functions alone

from pyspark.sql.functions import col

CustomersOrders = customers.join(ordersComplete, customers.customer_id == ordersComplete.order_customer_id, 'inner') \
.groupBy([customers.customer_id,customers.customer_fname]).count()

CustomersMoreThan4 = CustomersOrders \
.where(col('count') > 4) \
.sort(col('count').asc())

2)  Solution with RDD

orders = sc.textFile("/user/cloudera/p1/p4/orders")
customers = sc.textFile("/user/cloudera/p1/p4/customers")

ordersComplete = orders.filter(lambda rec : rec.split(",")[3] == 'COMPLETE')

ordersMap = ordersComplete.map(lambda rec: ( int(rec.split(",")[2]) , 1))
customersMap = customers.map(lambda rec: ( int(rec.split(",")[0]) , (int(rec.split(",")[0]),rec.split(",")[1])) )

CustomersJoin = customersMap.join(ordersMap)

CustomersMap2 = CustomersJoin.map(lambda rec: ( (rec[0], rec[1][0][1]) , rec[1][1] ) )

CustomersAgg = CustomersMap2.reduceByKey(lambda x,y : x+y)

CustomersMoreThan4RDD = CustomersAgg.filter(lambda rec : rec[1] > 4)

from pyspark.sql import Row
CustomersMoreThan4Map = CustomersMoreThan4RDD.map(lambda rec: (rec[1] , Row(customer_id=rec[0][0], customer_fname = rec[0][1], count = rec[1])) )
CustomersMoreThanSort = CustomersMoreThan4Map.sortByKey()

CustomersMoreThan4Final = CustomersMoreThanSort.map(lambda rec : rec[1])

FInalDF = CustomersMoreThan4Final.toDF()
Final = FInalDF.select('customer_id','customer_fname','count')

'''

