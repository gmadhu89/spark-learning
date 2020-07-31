'''
Problem Statement:

Data is available in local file system /data/retail_db
Source directories: /data/retail_db/orders and /data/retail_db/customers
Source delimiter: comma (“,”)
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname
Target Columns: customer_lname, customer_fname
Number of files - 1
Target Directory: /user/<YOUR_USER_ID>/solutions/solutions02/inactive_customers
Target File Format: TEXT
Target Delimiter: comma (“, ”)
Compression: N/A

'''

'''
Execution Command
spark2-submit --master yarn \
--conf spark.ui.port=12709 \
--executor-cores 2 \
--num-executors 6 \
--executor-memory 3G \
Get_Inactive_Customers_DF.py
'''

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Inactive customers solution using DF').getOrCreate()

ordersFile=open("/data/retail_db/orders/part-00000").read().splitlines()
customersFile=open("/data/retail_db/customers/part-00000").read().splitlines()

orders=spark.sparkContext.parallelize(ordersFile)
customers=spark.sparkContext.parallelize(customersFile)

from pyspark.sql import Row
from pyspark.sql.functions import col
ordersDF=orders.map(lambda o:Row(order_id=int(o.split(",")[0]),order_date=o.split(",")[1], order_customer_id=int(o.split(",")[2]),order_status = o.split(",")[3] )).toDF()
customersDF=customers.map(lambda o:Row(customer_id=int(o.split(",")[0]),customer_fname=o.split(",")[1], customer_lname=o.split(",")[2] )).toDF()

CustomerOrderJoin= customersDF.join(ordersDF, customersDF.customer_id==ordersDF.order_customer_id,'left_outer') \
.filter(col('order_id').isNull())

CustomerSort=CustomerOrderJoin.select('customer_lname','customer_fname') \
.orderBy(CustomerOrderJoin.customer_lname,CustomerOrderJoin.customer_fname)


path="/user/gmadhu89/solutions/solutions02/inactive_customersDF"
import subprocess
if subprocess.call(["hdfs","dfs","-test","-e",path]) == 0:
	subprocess.call(["hadoop","fs","-rm","-R",path])

CustomerSortRDD = CustomerSort.rdd
CustomerFinal=CustomerSortRDD.map(lambda rec: str(rec.customer_lname) + ", " + str(rec.customer_fname))

CustomerFinal.coalesce(1).saveAsTextFile("/user/gmadhu89/solutions/solutions02/inactive_customersDF")
